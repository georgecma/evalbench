from google.cloud import secretmanager_v1

def get_db_secret(secret_path):
    # Create a client
    client = secretmanager_v1.SecretManagerServiceClient()
    # Initialize request argument(s)
    request = secretmanager_v1.AccessSecretVersionRequest(
        name=secret_path,
    )
    # Make the request
    response = client.access_secret_version(request=request)
    # Return the secret
    return(response.payload.data.decode('utf-8'))
    
    
def generate_ddl(data, db_name, comments_data=None):
    ddl_statements = []
    current_table = None
    if comments_data is None:
        comments_data = dict()

    for table_name, column_name, data_type in data:
        if table_name != current_table:
            if current_table is not None:
                ddl_statements.append(';\n')  # End previous table statement
            ddl_statements.append(f'CREATE TABLE {table_name} (\n')
            current_table = table_name

        column_comment = None
        if len(comments_data) > 0:
            try:
                column_comment = comments_data[db_name][table_name][column_name]['column_description']
            except Exception as e:
                print(e)

        if column_comment is None:
            ddl_statements.append(f'    {column_name} {data_type},\n')
        else:
            ddl_statements.append(
                f'    {column_name} {data_type}, -- {column_comment} \n'
            )

    if current_table is not None:
        ddl_statements.append(');\n')  # End the last table statement

    return ''.join(ddl_statements)
