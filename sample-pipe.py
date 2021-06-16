from kfp import dsl
from kfp.v2 import compiler, components
from kfp.v2.dsl import (
    component,
    Input,
    Output,
    Dataset,
    Metrics,
)
from kfp.v2.google.client import AIPlatformClient
from google.cloud import aiplatform
from google_cloud_pipeline_components import aiplatform as gcc_aip


@component(packages_to_install=['google-api-python-client'])
def run_bq_query_job(project_id: str, query: str) -> str:
    import subprocess

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('bigquery', 'v2')

    query = query
    request_body = {
        "query": query,
        "useLegacySql": False
    }
    result = None
    try:
        result = client.jobs().query(
            projectId=project_id,
            body=request_body
        ).execute()
    except Exception as ex:
        raise Exception("Error starting BQ query job: {}".format(ex))

    if result:
        return result.get("jobReference").get("jobId")
    else:
        return 'Failed getting job ID for query job'


@component(packages_to_install=['google-api-python-client'])
def wait_for_job_status(job_id: str,
                        project_id: str,
                        timeout_s: int) -> str:
    import subprocess
    import time

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    def __get_job_status(project_id: str, job_id: str) -> str:
        client = googleapiclient.discovery.build('bigquery', 'v2')
        result = client.jobs().get(
            projectId=project_id,
            jobId=job_id
        ).execute()
        status = result.get("status")
        error_result = status.get("errorResult")
        if error_result is not None:
            raise Exception("BigQuery job with job_id={0} failed: {1}".format(job_id, status))
        return status.get("state")

    start_time = time.time()
    while True:
        status = __get_job_status(project_id=project_id, job_id=job_id)
        if status == "PENDING" or status == "RUNNING":
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_s:
                return "Waiting for BigQuery job with job_id={0}. Timeout. Waited time={1}s. Job status={2}"\
                    .format(job_id, round(elapsed_time, 2), status)
            time.sleep(5)
        else:
            break

    return 'success'


@component(packages_to_install=['google-api-python-client'])
def launch_dataflow_flex_template(result: str) -> str:
    import subprocess

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('dataflow', 'v1b3')

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

    return result.get("job").get("id")


@component(packages_to_install=['google-api-python-client'])
def launch_dataflow_template(job_name: str,
                             project_id: str,
                             num_workers: int,
                             max_workers: int,
                             location: str,
                             temp_location: str,
                             machine_type: str,
                             template_path: str,
                             params: dict) -> str:
    import subprocess

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('dataflow', 'v1b3')

    result = None
    try:
        result = client.projects().templates().launch(
            projectId=project_id,
            location=location,
            gcsPath=template_path,
            body={
                "jobName": job_name,
                "parameters": params,
                "environment":{
                    "numWorkers": num_workers,
                    "maxWorkers": max_workers,
                    "tempLocation": temp_location,
                    "machineType": machine_type
                }
            }
        ).execute()
    except Exception as ex:
        raise Exception('Error launching dataflow template: {}'.format(ex))

    if result:
        return result.get("job").get("id")
    else:
        raise Exception('No result launching dataflow template')


@component(packages_to_install=['google-api-python-client'])
def wait_for_dataflow_job_to_finish(job_id: str,
                                    project_id: str,
                                    location: str,
                                    timeout_s: int) -> str:
    import subprocess
    import time

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    def __get_job_status(project_id: str, job_id: str, location: str) -> str:
        client = googleapiclient.discovery.build('dataflow', 'v1b3')

        result = None
        try:
            result = client.projects().locations().jobs().get(
                projectId=project_id,
                location=location,
                jobId=job_id
            ).execute()
        except Exception as ex:
            raise Exception('Failed to get job status for id: {} with Error: {}'.format(job_id, ex))

        if result:
            return result.get("currentState")
        else:
            raise Exception('Failed getting result for job id: {}'.format(job_id))

    start_time = time.time()
    while True:
        status = __get_job_status(project_id=project_id, job_id=job_id, location=location)
        if status != "JOB_STATE_DONE":
            elapsed_time = time.time() - start_time
            if elapsed_time > timeout_s:
                return "Waiting for Dataflow job with job_id={0}. Timeout. Waited time={1}s. Job status={2}".format(job_id,round(elapsed_time,2),status)
            time.sleep(60)
        else:
            break

    return 'success'



@dsl.pipeline(
  name='batch-reload',
  description='An example pipeline that performs addition calculations.',
)
def batch_reload_pipeline(project_id_bq: str, query: str, project_id: str, location: str):

    # Start BQ query job and receive job id
    bq_job_id = run_bq_query_job(project_id=project_id_bq, query=query)

    # Wait for BQ to finish
    result = wait_for_job_status(job_id=bq_job_id.output, project_id=project_id_bq, timeout_s=900)

    # launch dataflow template
    dataflow_job_id = launch_dataflow_flex_template(result= result.output)

    # wait for datflow pipe to finish
    result_datflow = wait_for_dataflow_job_to_finish(job_id=dataflow_job_id.output, project_id=project_id, location=location, timeout_s=3600)


if __name__ == '__main__':

    project_id = 'vortex-sample'
    region = 'us-central1'
    pipeline_root_path = 'gs://vertex-gcs'

    compiler.Compiler().compile(pipeline_func=batch_reload_pipeline,
                                package_path='sample-pipe-desc.json'
                            )

    api_client = AIPlatformClient(project_id=project_id, region=region)

    response = api_client.create_run_from_job_spec(
        'sample-pipe-desc.json',
        pipeline_root=pipeline_root_path,
        parameter_values={'project_id_bq': 'spielwiese-tobias',
                          'query': '''DELETE FROM `spielwiese-tobias.test.my_new_table` where manufacturer = "VIVANCO"''',
                          'project_id': 'vortex-sample',
                          'location': 'europe-west1'
                          }
    )


    '''
    response = api_client.create_schedule_from_job_spec(
        'sample-pipe-desc.json',
        pipeline_root=pipeline_root_path,
        schedule='*/5 * * * *'
    )
    '''

