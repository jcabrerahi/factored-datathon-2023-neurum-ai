from azure.storage.filedatalake import DataLakeServiceClient

def initialize_storage_account(storage_account_name, storage_account_key):

    try:  
        global service_client

        service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=storage_account_key)

    except Exception as e:
        print(e)

def list_directory_contents(file_system_name, directory_name):

    try:
        file_system_client = service_client.get_file_system_client(file_system=file_system_name)

        paths = file_system_client.get_paths(path=directory_name)

        for path in paths:
            print(path.name + '\n')

    except Exception as e:
        print(e)

storage_account_name = "<tu-storage-account-name>"
storage_account_key = "<tu-storage-account-key>"

file_system_name = "<tu-file-system-name>"
directory_name = "<tu-directory-name>"

initialize_storage_account(storage_account_name, storage_account_key)
list_directory_contents(file_system_name, directory_name)
