
import kfp
from kfp import dsl
import subprocess
import os
import time
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

'''
def gcs_download_op(url):
    return dsl.ContainerOp(
        name='GCS - Download',
        image='google/cloud-sdk:279.0.0',
        command=['sh', '-c'],
        arguments=['gsutil cat $0 | tee $1', url, '/tmp/results.txt'],
        file_outputs={
            'data': '/tmp/results.txt',
        }
    )


def echo_op(text):
    return dsl.ContainerOp(
        name='echo',
        image='library/bash:4.4.23',
        command=['sh', '-c'],
        arguments=['echo "$0"', text]
    )

'''


@component(packages_to_install=['google-api-python-client'])
def bq_sample() -> str:
    import subprocess

    subprocess.run(["pip", "install", "google-api-python-client"])
    import googleapiclient.discovery

    client = googleapiclient.discovery.build('bigquery', 'v2')

    query = '''
        DELETE FROM `spielwiese-tobias.test.my_new_table` WHERE manufacturer = "klickTel AG"
    '''
    request_body = {
        "query": query,
        "useLegacySql": False
    }

    result = None
    try:
        result = client.jobs().query(
            projectId='spielwiese-tobias',
            body=request_body
        ).execute()
    except Exception as ex:
        raise Exception("my test error: {}".format(ex))

    if result:
        return result.get("jobReference").get("jobId")
    else:
        return 'failiure'


@component(packages_to_install=['google-api-python-client'])
def wait_for_job_status(job_id: str) -> str:
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
        status = __get_job_status(project_id='spielwiese-tobias', job_id=job_id)
        if status == "PENDING" or status == "RUNNING":
            elapsed_time = time.time() - start_time
            if elapsed_time > 900:
                return "Waiting for BigQuery job with job_id={0}. Timeout. Waited time={1}s. Job status={2}".format(job_id,round(elapsed_time,2),status)
            time.sleep(5)
        else:
            break

    return 'success'


def add(a: float, b: float) -> float:
  '''Calculates sum of two arguments'''
  return a + b



@dsl.pipeline(
  name='bq-pipeline',
  description='An example pipeline that performs addition calculations.',
)
def add_pipeline():

    # Passes a pipeline parameter and a constant value to the `add` factory
    # function.
    job_id = bq_sample()
    # Passes an output reference from `first_add_task` and a pipeline parameter
    # to the `add` factory function. For operations with a single return
    # value, the output reference can be accessed as `task.output` or
    # `task.outputs['output_name']`.
    output = wait_for_job_status(job_id.output)

# Specify pipeline argument values

if __name__ == '__main__':
    #client = kfp.Client(host='https://1f4b406cc9338a6d-dot-europe-west1.pipelines.googleusercontent.com')

    '''
    client.create_run_from_pipeline_func(
        sequential_pipeline,
        arguments={
            'url': 'gs://gcs-sample-th/th-test/sample.txt'
        })
    '''

    project_id = 'vortex-sample'
    region = 'us-central1'
    pipeline_root_path = 'gs://vertex-gcs'

    #kfp.compiler.Compiler().compile(sequential_pipeline, __file__ + '.yaml')

    compiler.Compiler().compile(pipeline_func=add_pipeline,
                                package_path='sample-pipe-desc.json'
                            )

    api_client = AIPlatformClient(project_id=project_id, region=region)

    response = api_client.create_run_from_job_spec(
        'sample-pipe-desc.json',
        pipeline_root=pipeline_root_path
    )


    '''
    response = api_client.create_schedule_from_job_spec(
        'sample-pipe-desc.json',
        pipeline_root=pipeline_root_path,
        schedule='*/5 * * * *'
    )
    '''

