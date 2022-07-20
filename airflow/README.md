# Airflow pipeline

The dag for this pipeline is in etl_dag.py. A start step was symbolically added as a dummy operator. All the data processing steps, have been written as python scripts stored in the folder scripts/. The end step is a simple `echo` that emulates a possible teardown process. A watcher function was defined as a step in order to keep track of any possible error which would trigger it's execution.

All data generated from this pipeline is dumped into scripts/dw/. Additional data is read from ext_sources/.

## Running the pipeline

This DAG runs upon creation or on an hourly basis. Each DAG execution will be recognized by its run_time value (current date time in YYYYMMDDHH format). The run_time value is used as partitions for all dumped data in parquet format.
  
Data is extracted from JSON, parquet and csv formats. Data transformations handled as pandas' dataframes. Handling of parquet format is made using pyarrow.

## DAG steps

1. **get_weather**
    - **Description:** Gets the weather data from [an API](https://smn.conagua.gob.mx/es/web-service-api) compressed as gz. It uncompresses the gz file, transforms it into pandas to define the run_time value as index, and dumps it into a parquet dataset partitioned by run_time value.
    - **Scripts**: scripts/api.py
    - **Sources:** api response
    - **Sinks:** dw/weather_api/*run_time value*
2. **append_muns**
    - **Description:** Takes the data dumped in the previous step, for both this run and the one corresponding to the immediately previous hour, loading them as dataframes. A composite id from joining with a hyphen the state and municipality keys. Then, an average aggregation is performed on tmax and tmin values, grouping the results by state-municipality. 
    - **Scripts**: scripts/municipalities.py
    - **Sources:** dw/weather_api/
    - **Sink:** dw/municipalities/*run_time value*
3. **enrich_current:** 
    - **Description:** Reads the data dumped in the previous and loads it as a dataframe. Reads data in csv format from the most recent ext_sources/, assuming names are in a YYYYMMDD date format, and also loads it as a dataframe. Both dataframes are merged using the state-municipality value as join keys. The enriched data is then dumped.
    - **Scripts**: scripts/current.py
    - **Sources:** dw/weather_api/ ext_sources/
    - **Sink:** dw/current/*run_time value*

## Additional comment

Airflow is a tool to meant to manage workflow, rather than specifically manipulating pipelines. One caveat is passing data from one step to the other; instead of processing it on the fly, data must be dumped in one step and then read again in the next one. In this particular workflow, the data from the second step, append_muns, isn't "pipelined" into the enrich_current step.
  However the main reasons that made me choose Airflow were:
- schedule options
- logging and monitoring tools
- scalable

About scalability, this Airflow workflow is meant to be a simple exercise, designed to run locally. If more complex and/or robust pipelines were needed, Airflow could be then used to trigger Beam or Spark jobs for optimal "pipelining", instead having to extract again what's just has been dumped. However, for this exercise, Airflow itself is kind of overkill.