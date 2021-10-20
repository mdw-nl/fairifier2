from datetime import timedelta, datetime
from pathlib import Path
import os
import logging
import xml.etree.ElementTree as et

from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import settings

import pandas as pd
import rdflib as rdf
import requests
import yaml

from sparql.query_engine import QueryEngine


def get_lc_ss_oid(lc_endpoint, lc_user, lc_password, study_identifier, ss_label):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(f'Trying to get SS_OID for {ss_label}')

    # Check if subject exists
    check_data = f'''
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/studySubject/v1" xmlns:bean="http://openclinica.org/ws/beans">
        <soapenv:Header>
        <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
        <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
        <wsse:Username>{lc_user}</wsse:Username>
        <wsse:Password type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{lc_password}</wsse:Password>
        </wsse:UsernameToken>
        </wsse:Security>
        </soapenv:Header>
        <soapenv:Body>
            <v1:isStudySubjectRequest>
                <v1:studySubject>
                    <bean:label>{ss_label}</bean:label>
                    <bean:studyRef>
                    <bean:identifier>{study_identifier}</bean:identifier>
                    </bean:studyRef>
                </v1:studySubject>
            </v1:isStudySubjectRequest>
        </soapenv:Body>
        </soapenv:Envelope>
    '''

    # Create the studysubject
    ret = requests.post(lc_endpoint + 'studySubject/v1/studySubjectWsdl.wsdl', data=check_data, headers={'Content-Type': 'text/xml'})
    retxml = et.fromstring(ret.text)
    retval = retxml[1][0][0].text
    LOGGER.info(f'Got return message {retval} on existance check')
    if retval == 'Success':
        LOGGER.info('Found existing SS_OID {retxml[1][0][1].text}')
        return retxml[1][0][1].text
    else:
        LOGGER.info('No existing SS_OID found, querying new one')

        create_data = f'''
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/studySubject/v1" xmlns:bean="http://openclinica.org/ws/beans">
                <soapenv:Header>
                <wsse:Security soapenv:mustUnderstand="1"
                xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <wsse:UsernameToken wsu:Id="UsernameToken-27777511"
                xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                <wsse:Username>{lc_user}</wsse:Username>
                <wsse:Password
                type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{lc_password}</wsse:Password>
                </wsse:UsernameToken>
                </wsse:Security>
            </soapenv:Header>

            <soapenv:Body>
                <v1:createRequest>
                    <v1:studySubject>
                        <!--Optional:-->
                        <bean:label>{ss_label}</bean:label>
                        <bean:enrollmentDate>2021-08-31</bean:enrollmentDate>
                        <bean:subject>
                        <!--Optional:-->
                        <bean:uniqueIdentifier>{ss_label}</bean:uniqueIdentifier>
                        <bean:gender>m</bean:gender>
                        <!--You have a CHOICE of the next 2 items at this level-->
                        <bean:dateOfBirth>1994-09-21</bean:dateOfBirth>
                        </bean:subject>
                        <bean:studyRef>
                        <bean:identifier>{study_identifier}</bean:identifier>
                        </bean:studyRef>
                    </v1:studySubject>
                </v1:createRequest>
            </soapenv:Body>
            </soapenv:Envelope>
        '''
        ret = requests.post(lc_endpoint + 'studySubject/v1/studySubjectWsdl.wsdl', data=create_data, headers={'Content-Type': 'text/xml'})
        print(f'Status code {ret.status_code}')
        if ret.status_code == 200:
            retxml = et.fromstring(ret.text)
            if retxml[1][0][0].text == 'Success':
                # now get the actual ID...
                check_data = f'''
                    <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/studySubject/v1" xmlns:bean="http://openclinica.org/ws/beans">
                    <soapenv:Header>
                    <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                    <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                    <wsse:Username>{lc_user}</wsse:Username>
                    <wsse:Password type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{lc_password}</wsse:Password>
                    </wsse:UsernameToken>
                    </wsse:Security>
                    </soapenv:Header>
                    <soapenv:Body>
                        <v1:isStudySubjectRequest>
                            <v1:studySubject>
                                <bean:label>{ss_label}</bean:label>
                                <bean:studyRef>
                                <bean:identifier>{study_identifier}</bean:identifier>
                                </bean:studyRef>
                            </v1:studySubject>
                        </v1:isStudySubjectRequest>
                    </soapenv:Body>
                    </soapenv:Envelope>
                '''

                ret = requests.post(lc_endpoint + 'studySubject/v1/studySubjectWsdl.wsdl', data=check_data, headers={'Content-Type': 'text/xml'})
                retxml = et.fromstring(ret.text)
                if retxml[1][0][0].text == 'Success':
                    return retxml[1][0][1].text


def upload_to_lc(sparql_endpoint, query, lc_endpoint, lc_user, lc_password, study_oid, study_identifier, event_oid, form_oid, item_group_oid, identifier_colname, item_prefix, alternative_item_oids={}, **kwargs):
    LOGGER = logging.getLogger("airflow.task")
    LOGGER.info(f'Retrieving triples from {sparql_endpoint}')
    sparql = QueryEngine(sparql_endpoint)
    df: pd.DataFrame = sparql.get_sparql_dataframe(query)
    LOGGER.info(f'Received {len(df)} rows of data')

    df = df.rename(columns=alternative_item_oids)

    LOGGER.info(f'Have columns {df.columns}')

    subjects = []

    for _, row in df.iterrows():
        id = row[identifier_colname]
        LOGGER.debug(f'Adding subject {id}')

        # Get SS OID
        ss_id = get_lc_ss_oid(lc_endpoint, lc_user, lc_password, study_oid, id)
        LOGGER.debug(f'Got SS_OID {ss_id}')

        # Make sure the event is scheduled
        schedule_body = f'''
            <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/event/v1" xmlns:bean="http://openclinica.org/ws/beans">
            <soapenv:Header>
                <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                <wsse:Username>{lc_user}</wsse:Username>
                <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{lc_password}</wsse:Password>
                </wsse:UsernameToken>
                </wsse:Security>
            </soapenv:Header>
            <soapenv:Body>
                <v1:scheduleRequest>
                    <v1:event>
                        <bean:studySubjectRef>
                        <bean:label>{ss_id}</bean:label>
                        </bean:studySubjectRef>
                        <bean:studyRef>
                            <bean:identifier>{study_oid}</bean:identifier>
                        </bean:studyRef>
                        <bean:eventDefinitionOID>{event_oid}</bean:eventDefinitionOID>
                        <bean:startDate>2020-01-01</bean:startDate>
                    </v1:event>
                </v1:scheduleRequest>
            </soapenv:Body>
            </soapenv:Envelope>
        '''

        ret = requests.post(lc_endpoint + 'event/v1/eventWsdl.wsdl', data=schedule_body, headers={'Content-Type': 'text/xml'})
        LOGGER.debug(f'Got return code {ret.status_code} for scheduling the event')

        items = ''
        for name in df.columns.names:
            if name != identifier_colname:
                if row[name] is not None:
                    items += f'<ItemData ItemOID="{item_prefix}{name}" Value="{row[name]}"/>\n'
        
        subject = f'''
            <SubjectData SubjectKey="{ss_id}">
                <StudyEventData StudyEventOID="{event_oid}" StudyEventRepeatKey="1">
                    <FormData FormOID="{form_oid}" OpenClinica:Status="initial data entry">
                        <ItemGroupData ItemGroupOID="{item_group_oid}" ItemGroupRepeatKey="1" TransactionType="Insert">
                            {items}
                        </ItemGroupData>
                    </FormData>
                </StudyEventData>
            </SubjectData>
        '''
        subjects.append(subject)

    LOGGER.info(f'Starting upload for {len(subjects)} subjects')

    newline='\n'
    submit_data = f'''
        <soapenv:Envelope xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/" xmlns:v1="http://openclinica.org/ws/data/v1" xmlns:OpenClinica="http://www.openclinica.org/ns/odm_ext_v130/v3.1">
        <soapenv:Header>
            <wsse:Security soapenv:mustUnderstand="1" xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
                <wsse:UsernameToken wsu:Id="UsernameToken-27777511" xmlns:wsu="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-utility-1.0.xsd">
                    <wsse:Username>{lc_user}</wsse:Username>
                    <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{lc_password}</wsse:Password>
                </wsse:UsernameToken>
            </wsse:Security>
        </soapenv:Header>

        <soapenv:Body>
        <v1:importRequest>
        <odm>
        <ODM>
        <ClinicalData StudyOID="{study_oid}" MetaDataVersionOID="v1.0.0">
            <UpsertOn NotStarted="true" DataEntryStarted="true" DataEntryComplete="true"/>
            {newline.join(subjects)}
        </ClinicalData>
        </ODM>
        </odm>
        </v1:importRequest>  
        </soapenv:Body>
        </soapenv:Envelope>
    '''

    ret = requests.post(lc_endpoint + 'data/v1/dataWsdl.wsdl', data=submit_data, headers={'Content-Type': 'text/xml'})
    LOGGER.info(f'Got return code {ret.status_code} for upload')


def create_dag(dag_id,
               schedule,
               default_args,
               upload_kwargs):

    dag = DAG(
        dag_id,
        default_args=default_args,
        schedule_interval=schedule,
        start_date=days_ago(0),
        catchup=False,
        max_active_runs=1
    )

    op_kwargs = upload_kwargs
    op_kwargs['sparql_endpoint'] = os.environ['SPARQL_ENDPOINT']
    op_kwargs['lc_endpoint'] = os.environ['LC_ENDPOINT']
    op_kwargs['lc_user'] = os.environ['LC_USER']
    op_kwargs['lc_password'] = os.environ['LC_PASSWORD']

    with dag:
        t1 = PythonOperator(
            task_id='push_lc',
            python_callable=upload_to_lc,
            op_kwargs=upload_kwargs)

    return dag

filename = os.environ['LC_UPLOAD_CONFIG']
with open(filename) as f:
    upload_config = yaml.load(f)

for key, val in upload_config.items():
    dag_id = f'lc_upload_list_{key}'

    default_args = {
        'owner': 'airflow',
        'depends_on_past': False,
        'retries': 0,
    }

    globals()[dag_id] = create_dag(dag_id=dag_id,
                                schedule=None,
                                default_args=default_args,
                                upload_kwargs=val)