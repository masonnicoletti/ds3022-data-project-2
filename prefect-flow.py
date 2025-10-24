# prefect flow goes here
from prefect import task, flow

@task
def start():
    print("Hello World")

@flow
def start_flow():
    start()

if __name__ == "__main__":
    start_flow()
