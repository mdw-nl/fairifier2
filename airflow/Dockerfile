#FROM apache/airflow:slim-2.8.0-python3.9
#USER root
#RUN mkdir -p /usr/share/man/man1
#RUN apt-get update && \
#    apt-get install -y default-jdk && \
#    apt-get install -y git && \
#    apt-get install -y build-essential &&\
#    apt-get clean
#USER airflow
#
#COPY ./ontop-cli-4.1.0/* /usr/local/airflow/R2RML/R2RML-cli/
#ENV AIRFLOW_VAR_R2RML_CLI_DIR=/usr/local/airflow/R2RML/R2RML-cli
#
#COPY ./requirements.txt ./requirements.txt
#RUN pip3 install -r requirements.txt

FROM apache/airflow:2.8.1


USER root
RUN mkdir -p /usr/share/man/man1
RUN apt-get update && \
    apt-get install -y git && \
    apt-get install -y build-essential &&\
    apt-get clean
USER airflow



COPY ./requirements.txt ./requirements.txt
RUN pip3 install -r requirements.txt