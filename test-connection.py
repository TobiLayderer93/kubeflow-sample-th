
import kfp
client = kfp.Client(host='https://1f4b406cc9338a6d-dot-europe-west1.pipelines.googleusercontent.com')

if __name__ == '__main__':
    client.list_pipelines()
