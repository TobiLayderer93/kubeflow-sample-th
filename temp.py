

if __name__ == '__main__':
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('bigquery', 'v2')

    query = '''
            DELETE FROM `spielwiese-tobias.test.my_new_table` WHERE manufacturer = "GORENJE"
        '''
    request_body = {
        "query": query,
        "useLegacySql": False
    }

    try:
        result = client.jobs().query(
            projectId='spielwiese-tobias',
            body=request_body
        ).execute()
    except Exception as ex:
        print('error')

    print(result)
