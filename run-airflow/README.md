# Run Apache Airflow

v3.0.1

Use this to run Airflow locally in a development environment. NOT recommended for any production use cases.

## Start Airflow

1. Clone this repository and `cd` into it in your terminal.
2. Run the following command to bring up the Airflow stack:

    ```
    docker compose up -d
    ```

3. Once the various containers, storage volumes, and network is created, open a browser to http://127.0.0.1:8080/
4. You can sign into the web UI with the username `airflow` and password `airflow`.
5. Add/manage your DAGs by placing them within the `dags/` subdirectory of this project.

## Stop Airflow

From the same (root) directory of this project as above, issue this command in your terminal:

```
docker compose down
```

## Setup and Working with AWS Credentials

[![Run Airflow](https://s3.amazonaws.com/uvasds-systems/images/run-airflow-youtube.png)](https://www.youtube.com/watch?v=muofVU8gkEQ)
