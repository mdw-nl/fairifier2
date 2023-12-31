# FAIRifier 2.0

The goal of the FAIRifier is to FAIRify tabular data. 
This tooling focuses on the I and R of FAIR, and achieves this by turning tabular data into RDF data.

The tool is based on a few pipelines defined in python files (under `airflow/dags`), 
which are responsible for different parts of the transformation.

## Configuration

Configuration is mostly done through environment variables, which are loaded into the Airflow workers.
`.env.example` has a list of all the necessary configuration options. 
Note that all of these can also be defined in Airflow itself, under `Variables`.

## Services

* Postgres: port 5432
* Airflow: port 8080
* GraphDB: port 7200
* Dash UI: port 5050

## DAGs

The code for all the DAGs is available under `airflow/dags`.

### `upload_csv_dag`

You can upload CSV's or excel files to a postgres database using the user interface.
These files will automatically be uploaded to the defined postgres database.

The tabular data will be uploaded to tables in the database equal to their name (minus the extension). 
The database used is defined by the Airflow Variable `R2RML_DB_URL`, by default this is set to a 
database `data` in the built-in postgres container.

If data has been inserted in a table before, a new upload can either replace said data or append to it.
This is handled by the `APPEND_CSV` Airflow Variable.

After uploading, the DAG will trigger the `generate_triples_dag` automatically to refresh triples stored.

### `generate_triples_dag`

This DAG does the following things:

* Clone the repository defined by `R2RML_REPO`
* Go into subdirectory of that repo defined by `R2RML_REPO_SUBDIR`, by default it stays at the root
* Grab all `.ttl` files from that directory and assume they are R2RML files
* For each of the R2RML files grabbed this way:
    * Using database defined by `R2RML_RDB_CONNSTR`, `R2RML_RDB_PASSWORD`, and `R2RML_RDB_USER`
    ~~* Using ontop~~
    * Using morph-kgc library
    * Generate a KG with kglab library that is passed 
    ~~* Generate a `.ttl` file containing RDF data. The file has the same filename as the input R2RML file~~
    * Upload this `.ttl` file to the GraphDB instance defined by `SPARQL_ENDPOINT` (in a graph with the same name as the .ttl file)
* Finally download any ontologies defined in the `airflow/generate_triples_dag.py` file and upload them to a graph in the GraphDB instance

It then cleans up all the intermediary steps.
