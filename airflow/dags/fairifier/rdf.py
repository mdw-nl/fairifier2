from pathlib import Path
import logging
from typing import Optional, Dict
import rdflib as rdf
from SPARQLWrapper import SPARQLWrapper, POSTDIRECTLY, JSON
import os
from airflow.operators.bash import BashOperator
import kglab


# TODO Adjust rdf_conversion function passing db info
def upload_terminology(url, sparql_endpoint, format='nt', **kwargs):
    """Uploads a given ontology file to the SPARQL endpoint
    
    """
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(f'downloading file {url}')

    sparql = SPARQLWrapper(sparql_endpoint + '/statements')

    LOGGER.info(f'starting upload to {sparql_endpoint}')
    kg = kglab.KnowledgeGraph()

    # Load RDF data from the specified URL and format
    kg.load_rdf(url, format="xml")
    rdf_data = kg.save_rdf_text(format="nt")

    sparql.setMethod('POST')
    sparql.setQuery(query="""
        INSERT DATA {
            GRAPH <http://localhost/ontology> {
                %s
            } 
        }
        """ % rdf_data)
    response = sparql.query()
    LOGGER.info(f"Response status code: {response.response.code}")
    LOGGER.info(response.response.read().decode())


def upload_rdf(rdf_data, sparql_endpoint):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(f'uploading file data to {sparql_endpoint}')
    LOGGER.info(type(rdf_data))

    sparql = SPARQLWrapper(sparql_endpoint + '/statements')
    sparql.setMethod('POST')
    if True:
        LOGGER.info("Dropping all existing graphs")
        deleteQuery = """
                DROP NAMED
            """

        sparql.setQuery(deleteQuery)
        sparql.query()

    sparql.setMethod('POST')

    # Set the RDF data to be uploaded
    sparql.setQuery(f"""
            INSERT DATA {{
                {rdf_data}
            }}
        """)

    # Execute the SPARQL Update query
    sparql.setReturnFormat(JSON)

    response = sparql.query()
    print(f"Response status code: {response.response.code}")
    print(response.response.read().decode())
    # Check the response status
    if response.response.code == 200:
        print("RDF data uploaded successfully to GraphDB.")
    else:
        print(f"Error uploading RDF data. Status code: {response.response.code}")
        print(response.response.read().decode())


def rdf_conversion(workdir,
                   r2rml_cli_dir,
                   rdb_connstr,
                   rdb_user,
                   rdb_pass):
    """

    Args:
        workdir:

    Returns:

    """
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.warning(f'Work dir path:{workdir}')
    LOGGER.warning(f'Work dir path:{rdb_connstr}')

    config = "[CONFIGURATION]" + "\nnumber_of_processes=1\n" + \
             "na_values=,#N/A,N/A,#N/A N/A,n/a,NA,<NA>,#NA,NULL,null,NaN,nan,None\n" + \
             f"output_dir={workdir}/output/\n" + \
             "[DataSource1]" + f"\nmappings={workdir}/ttl/mapping.ttl" + \
             f"\ndb_url= postgresql://{rdb_user}:{rdb_pass}@postgres:5432/data"
    LOGGER.info(f'Config file content:\n {config}')
    LOGGER.info(f'Creating output dir: {workdir}/output/')

    # Create directory
    os.makedirs(workdir + "/output/")
    input_path = Path(f"{workdir}/output/")
    LOGGER.info(f"File: {input_path}")
    # Generate KG
    LOGGER.info("Materialization....")
    kg = kglab.KnowledgeGraph()
    kg.materialize(config)
    LOGGER.info("Materialization completed!")
    kgs = kg.save_rdf_text(format="nt")
    return kgs
