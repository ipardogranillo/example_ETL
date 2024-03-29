# Weather ETL

## Objective
This ETL uses airflow to get data from [this API](https://smn.conagua.gob.mx/es/web-service-api) and process it.
1. Retrieve last record from API each hour.
2. Gather the most recent and historical data retrieved from the API in a folder.
3. Have a table for each municipality, where maximum temperature and minimum temperature is averaged over 2 hour windows.
4. Inside the most recent subfolder (named by date in a "YYYYMMDD" format), within a folder named "data_municipios", enrich each of the municipalities' tables from the previous step. Store the resulting tables in a folder named "current".

Also, the ETL should:
- be built using Docker and Docker Compose
- have logs

## Folders' contents
- airflow/ docker-compose.yaml and main ETL folder
  - dags/ dag for the ETL process; scripts and data nested within
    - scripts/ python scripts that define each dag step (ignored by Airflow)
      - dw/ folder where to dump parquets (ignored by git)
      - ext_sources/ folder where example csv data is loaded
  - logs/ auomatically generatd by Airflow and where its logs will be stored (ignored by git)
  - plugins/ automatically generated by Airflow (ignored by git)

## Setup
This pipeline was developed in Ubuntu 20.04 using [Docker](https://docs.docker.com/engine/install/ubuntu/), [Docker desktop](https://docs.docker.com/desktop/install/ubuntu/), and [Apache Airflow](https://airflow.apache.org/) to manage the ETL workflow.

### Docker versions
Docker and Docker Desktop version:
```
Client: Docker Engine - Community
 Cloud integration: v1.0.24
 Version:           20.10.17
 API version:       1.41
 Go version:        go1.17.11
 Git commit:        100c701
 Built:             Mon Jun  6 23:02:57 2022
 OS/Arch:           linux/amd64
 Context:           desktop-linux
 Experimental:      true

Server: Docker Desktop 4.10.1 (82475)
 Engine:
  Version:          20.10.17
  API version:      1.41 (minimum version 1.12)
  Go version:       go1.17.11
  Git commit:       a89b842
  Built:            Mon Jun  6 23:01:23 2022
  OS/Arch:          linux/amd64
  Experimental:     false
 containerd:
  Version:          1.6.6
  GitCommit:        10c12954828e7c7c9b6e0ea9b0c02b01407d3ae1
 runc:
  Version:          1.1.2
  GitCommit:        v1.1.2-0-ga916309
 docker-init:
  Version:          0.19.0
  GitCommit:        de40ad0
```

Docker Compose
```
docker-compose version 1.25.0, build unknown
docker-py version: 4.1.0
CPython version: 3.8.10
OpenSSL version: OpenSSL 1.1.1f  31 Mar 2020
```

### Building with Docker compose

After cloning this repository, change into the folder with docker-compose.yaml:
```
cd example_ETL/airflow/
```

Add your user id to access Airflow:
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
```

Make sure port 8080 is free:
```
fuser -k 8080/tcp
```

Use docker compose:
```
docker compose up
```

If successful, the terminal should start printing health checks approximately each 10 seconds:
```
etl-airflow-webserver-1  | 127.0.0.1 - - [16/Jul/2022:19:09:04 +0000] "GET /health HTTP/1.1" 200 187 "-" "curl/7.74.0"
```
## Running the pipeline in Airflow

Then Airflow can be accesed at http://localhost:8080. Login credentials are "airflow" for both username and password by default.  
  ![Image](https://user-images.githubusercontent.com/89820099/179369042-929bcb84-b34b-44e7-a949-37baba6d8256.png)  
  
The pipeline dag should be running once created; afterwards, it will run hourly.  
  ![Image](https://user-images.githubusercontent.com/89820099/179388650-bcb19cbb-7c64-485c-873a-5e6aa069bf87.png)  

  ![Image](https://user-images.githubusercontent.com/89820099/179416252-ef52e332-16dd-47e5-9dd6-4356a57e9eae.png)  

  Through [Airflow's UI](https://airflow.apache.org/docs/apache-airflow/stable/ui.html) you can view details of the whole pipeline's executions, including logs in each as details in each of the dag's tasks.
  
For more detail about what the pipeline see [its documentation](https://github.com/ipardogranillo/example_ETL/blob/main/airflow/README.md)