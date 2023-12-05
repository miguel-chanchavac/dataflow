# dataflow
### Create a virtual env
python -m venv env

### Activate virtual env 
source env/bin/activate

### Install Airflow
export AIRFLOW_HOME=/Users/mangeluz/airflow

AIRFLOW_VERSION=2.6.2
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

### The airflow standalone command initializes the database, creates a user, and starts all components.
airflow standalone

Si existe una base de datos en msqlite se debe borrar el archivo airflow.db para crearlo de nuevo con el comando de abajo, este solo se debe correr una vez para crear la base y las tablas.

airflow db init -> solo 1 vez para crear las tablas si esto ya se ejecuto no ejecutar 2 veces
airflow db check -> para chequear la connexion a la base SQLite for default

### turn on services

airflow webserver -p 8080 -D
airflow scheduler -D

### Later to go to locahost
localhost:8080

### Load a variable with export option another option is put this in .bash_profile
export SPARK_HOME=/Users/mangeluz/airflow/env

### must put the correct path for jars libraries about spark
pyspark --jars /Users/mangeluz/airflow/env/lib/python3.8/site-packages/pyspark/jars

