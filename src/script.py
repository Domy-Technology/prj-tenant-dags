import logging, os, traceback, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__),"../../../" )))

from bson import ObjectId
from colorama import Fore, Style
from src.client.database import DatabaseClient
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from src.utils.access_token import AccessTokenManager
from src.utils.utilities import env
from src.service.account.usecases.discovery import DiscoveryUseCase
from src.service.account.usecases.user import UserUseCase
from src.service.account.usecases.group import GroupUseCase
from src.service.account.usecases.principal import ServicePrincipalUseCase
from src.service.account.usecases.application import ApplicationUseCase
from src.service.account.usecases.role import DirectoryRoleUseCase

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# Configuration (use environment variables or a config file in production)
SOURCE_TENANT_ID = env("SOURCE_TENANT_ID")
SOURCE_CLIENT_ID = env("SOURCE_CLIENT_ID")
SOURCE_CLIENT_SECRET = env("SOURCE_CLIENT_SECRET")
SOURCE_DOMAIN = env("SOURCE_DOMAIN")

TARGET_TENANT_ID = env("TARGET_TENANT_ID")
TARGET_CLIENT_ID = env("TARGET_CLIENT_ID")
TARGET_CLIENT_SECRET = env("TARGET_CLIENT_SECRET")
TARGET_DOMAIN = env("TARGET_DOMAIN")

MONGODB_CONNECTION = env("MONGODB_CONNECTION")
MONGODB_DATABASE = env('MONGODB_DATABASE', "MIGRATION")

default_args = {'owner': 'airflow', 'depends_on_past': True }
dag = DAG('migration-account', default_args=default_args, description='Migration Account Migration', schedule_interval=None)

def run_account_migration():
    try:
        migration = AccountMigration()
        migration.start_migration()
    except Exception as e:
        logging.error(f"Migration failed: {str(e)}")
        logging.error(traceback.format_exc())
        raise e

migration_task = PythonOperator(
    task_id='run_account_migration',
    python_callable=run_account_migration,
    dag=dag
)

class AccountMigration:
    """
    Handles the entire migration process from source to target tenant.
    """

    def __init__(self):
        self.database = DatabaseClient(MONGODB_CONNECTION, MONGODB_DATABASE)
        self.migration_id = env('MIGRATION_ID', None)

    def start_migration(self):
        """
        Executes the migration process, handling errors and logging status updates.
        """
        try:
            source_token_manager, target_token_manager = self._get_tenant_tokens()

            if self._check_discovery():
                self._start_migration()
                self._migrate_users(target_token_manager)
                self._migrate_groups(target_token_manager)
                self._migrate_applications(target_token_manager)
                self._migrate_service_principals(target_token_manager)
                self._migrate_directory_roles(target_token_manager)
                self._finalize_migration(success=True)
            else:
                self._discovery(source_token_manager)

        except Exception as e:
            self._handle_migration_failure(e)
        finally:
            self.database.close()

    def _check_discovery(self) -> bool:
        """
        Checks if discovery has already been completed for the current migration.

        Returns:
            bool: True if account_status is present and truthy, otherwise False.
        """
        if self.migration_id:
            migration = self.database.read_one("migrations", {"_id": ObjectId(self.migration_id)})

            if bool(migration.get("account_status")) and "PENDING" or "COMPLETED" in migration.get("account_status"):
                return True
        
        return False

    def _discovery(self, token_manager: AccessTokenManager):
        """
        Discovery Account datas.
        """
        migration_title = f"Migration from {SOURCE_DOMAIN} to {TARGET_DOMAIN}"
        data = { "title": migration_title, "account_status": "DISCOVERY", "teams_status": None, "onedrive_status": None, "exchange_status": None, "sharepoint_status": None, "account_description": "Discovery Account has started", "teams_description": None, "onedrive_description": None, "exchange_description": None, "sharepoint_description": None, }
        self.migration_id = self.database.insert(collection="migrations", data=data)
        logging.info(f"{Fore.GREEN} {self.migration_id} - {migration_title} {Style.RESET_ALL}")
        
        discovery_usecase = DiscoveryUseCase(self.database, self.migration_id, token_manager)
        discovery_usecase.run_discovery()
        
        self.database.update(collection="migrations", query={"_id": ObjectId(self.migration_id)}, data={"account_status": "PENDING", "description": "Migration Account ready for execution"})

    def _get_tenant_tokens(self):
        """
        Retrieves access tokens for the source and target tenants.
        """
        logging.info(f"{Fore.GREEN} Retrieving Source Tenant Token {Style.RESET_ALL}")
        source_token_manager = AccessTokenManager(SOURCE_TENANT_ID, SOURCE_CLIENT_ID, SOURCE_CLIENT_SECRET)

        logging.info(f"{Fore.GREEN} Retrieving Target Tenant Token {Style.RESET_ALL}")
        target_token_manager = AccessTokenManager(TARGET_TENANT_ID, TARGET_CLIENT_ID, TARGET_CLIENT_SECRET)

        return source_token_manager, target_token_manager

    def _start_migration(self):
        """
        Updates the migration status in MongoDB and logs completion.
        """
        self.database.update(collection="migrations", query={"_id": ObjectId(self.migration_id)}, data={"account_status": "STARTED", "description": "Account migration has started"})
        logging.info(f"{Fore.GREEN} Account migration has STARTED {Style.RESET_ALL}")

    def _migrate_users(self, token_manager: AccessTokenManager):
        """
        Migrates users and returns a mapping of source UPNs to target user IDs.
        """
        logging.info(f"{Fore.GREEN} Getting Users from Target {SOURCE_DOMAIN} {Style.RESET_ALL}")
        user_usecase = UserUseCase(self.database, self.migration_id, token_manager)
        return user_usecase.migrate_users()

    def _migrate_groups(self, token_manager: AccessTokenManager):
        """
        Migrates groups and associates users.
        """
        logging.info(f"{Fore.GREEN} Getting Groups from {SOURCE_DOMAIN} {Style.RESET_ALL}")
        group_usecase = GroupUseCase(self.database, self.migration_id, token_manager)
        return group_usecase.migrate_groups()

    def _migrate_applications(self, token_manager: AccessTokenManager):
        """
        Migrates applications.
        """
        logging.info(f"{Fore.GREEN} Getting Applications from {SOURCE_DOMAIN} {Style.RESET_ALL}")
        application_usecase = ApplicationUseCase(self.database, self.migration_id, token_manager)
        return application_usecase.migrate_applications()

    def _migrate_service_principals(self, token_manager: AccessTokenManager):
        """
        Migrates service principals.
        """
        logging.info(f"{Fore.GREEN} Getting Service Principals from {SOURCE_DOMAIN} {Style.RESET_ALL}")
        service_principals_usecase = ServicePrincipalUseCase(self.database, self.migration_id, token_manager)
        return service_principals_usecase.migrate_service_principals()

    def _migrate_directory_roles(self, token_manager: AccessTokenManager):
        """
        Migrates directory roles and their assigned users.
        """
        logging.info(f"{Fore.GREEN} Getting Directory Roles from {SOURCE_DOMAIN} {Style.RESET_ALL}")
        application_usecase = DirectoryRoleUseCase(self.database, self.migration_id, token_manager)
        return application_usecase.migrate_roles()

    def _finalize_migration(self, success=True):
        """
        Updates the migration status in MongoDB and logs completion.
        """
        status = "COMPLETED" if success else "FAILED"
        description = "Account migration successfully finished" if success else "Account migration failed"

        self.database.update(collection="migrations", query={"_id": ObjectId(self.migration_id)}, data={"account_status": status, "description": description})
        logging.info(f"{Fore.GREEN} Account migration {status} {Style.RESET_ALL}")

    def _handle_migration_failure(self, error):
        """
        Handles migration failures, logs the error, and updates the migration status.
        """
        error_message = f"An error occurred during migration: {str(error)}"
        logging.error(f"{Fore.RED} {error_message} {Style.RESET_ALL}")
        logging.error(traceback.format_exc())

        self._finalize_migration(success=False)

if __name__ == "__main__":
    migration_manager = AccountMigration()
    migration_manager.start_migration()
