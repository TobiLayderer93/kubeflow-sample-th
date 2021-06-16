

def launch(client):
    result = client.projects().locations().flexTemplates().launch(
        projectId='vortex-sample',
        location='europe-west1',
        body={"launchParameter": {
            "jobName": "sample",
            "parameters": {
                "tableRef": "spielwiese-tobias:test.my_new_table",
                "bucket": "gs://vertex-gcs",
                "numShards": "10",
                "fields": "product_id,ean,subsidiary"
            },
            "containerSpecGcsPath": "gs://dataflow-templates/latest/flex/BigQuery_to_Parquet"
        }}
    ).execute()
    #result.get("job").get("id")
    print(result)

def get_dataflow_job(job_id):

    result = client.projects().locations().jobs().get(
        projectId='vortex-sample',
        location='europe-west1',
        jobId=job_id
    ).execute()
    #result.get("currentState")
    print(result)

if __name__ == '__main__':
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('dataflow', 'v1b3')

    #launch(client)
    get_dataflow_job('2021-06-16_01_03_36-17460218073891160619')

    #2021-06-16_00_42_00-11764626317786107316
