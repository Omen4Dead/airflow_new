FROM apache/airflow
COPY requirements.txt /opt/app/
WORKDIR /opt/app/
RUN pip install -r requirements.txt