from azure.storage.filedatalake import DataLakeServiceClient


def initialize_storage_account(storage_account_name_local: str, storage_account_key_local: str):
    service_client_local = None
    try:
        service_client_local = DataLakeServiceClient(
            account_url = f"https://{storage_account_name_local}.dfs.core.windows.net",
            credential=storage_account_key_local,
        )

    except ValueError as value_error:
        print("ValueError occurred:", value_error)

    except Exception as error:
        print("An unexpected error occurred:", error)

    return service_client_local


def list_directory_contents(service_client_local, file_system_name_local,
                            directory_name_local):
    try:
        file_system_client = service_client_local.get_file_system_client(
            file_system=file_system_name_local
        )

        paths = file_system_client.get_paths(path=directory_name_local)

        for path in paths:
            print(path.name + "\n")

    except Exception as error:
        print(error)


STORAGE_ACCOUNT_NAME = "<tu-storage-account-name>"
STORAGE_ACCOUNT_KEY = "<tu-storage-account-key>"

FILE_SYSTEM_NAME = "<tu-file-system-name>"
DIRECTORY_NAME = "<tu-directory-name>"

service_client = initialize_storage_account(STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_KEY)
list_directory_contents(service_client, FILE_SYSTEM_NAME, DIRECTORY_NAME)
