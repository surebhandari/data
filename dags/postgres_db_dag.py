from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
import json

def get_current_total_for_business(business_id):
    sql_stmt = "SELECT total FROM total where document_id = " + str(business_id)
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='veryfidb'
    )

    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    rs =  cursor.fetchall()
    print('inside get current batch id')
    print(rs)
    print(type(rs))
    return rs

def get_current_batch_id():
    sql_stmt = "SELECT * FROM tracker"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='veryfidb'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    rs =  cursor.fetchall()
    print('inside get current batch id')
    print(rs)
    return rs

def update_next_batch_id(total_records):
    print('inside update next batch id')
    next_batch_id = get_current_batch_id()[0][0] + total_records
    print(next_batch_id)
    sql_stmt = "UPDATE tracker SET batch_size = " + str(next_batch_id)
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='veryfidb'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    pg_conn.commit()

def get_veryfi_data():
    # getting batch of records
    # get the current batch_size which represents the start of document_id from 
    # where we need to process next 2000 records
    # once the processing is done, update the tracker table with 
    # the next batch starting document_id.
    print('first call get_current_batch_id')
    id_list = get_current_batch_id()
    print('id_list should be a list')
    print(type(id_list))
    print('batch start should be the second element of list')
    batch_start = id_list[0][0]
    print("batch_start = ")
    print(batch_start)
    batch_end = batch_start + 2000
    print("batch_end = ")
    print(batch_end)
    sql_stmt = "SELECT * FROM documents where document_id >="+ str(batch_start) + " and document_id <=" + str(batch_end) 
    print(sql_stmt)
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='veryfidb'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

def process_veryfi_data(ti):
    docs = ti.xcom_pull(task_ids=['get_veryfi_data'])
    print('I am expecting only one document')
    print(docs)
    if not docs:
        raise Exception('No docs to process')
    # process the records and create a map of business_id and sum total of values
    # return this map to another task that will upsert this data to another table
    print('printing docs123')
    # docs is formatted as [[[ ]]]
    records = docs[0]
    print('total records to process = ', len(records))
    post_map = {}
    for record in records:
        json_object = json.loads(record[1])
        # we should get the current total value for this document_id
        # and start adding to it.
        print('current business id =' + str(json_object['business_id']))
        if(len(get_current_total_for_business(json_object['business_id'])) != 0):
            total = int(get_current_total_for_business(json_object['business_id'])[0][0])
        else:
            total = 0
        print("starting total  = " + str(total))
        # json_object['total'] can be a list or dictionary
        business_id = record[0]
        if json_object['total'] and type(json_object['total']) == dict:
            total = total + int(json_object['total']['value'])

        if json_object['total'] and type(json_object['total']) == list:
            for item in json_object['total']:
                if item is not None:
                    total = total + int(item['value'])
        
        post_map[json_object['business_id']] = total
    # after this point update the next batch size in tracker table
    if(len(records) != 0):
        # only in this case update the next batch id
        update_next_batch_id(len(records))
    return post_map

def write_veryfi_data(ti):
    docs = ti.xcom_pull(task_ids=['process_veryfi_data'])
    # take a connection and upsert the data using sql
    # iterate through the map and for each key, write an upsert 
    # to the target database
    print('printing from put_veryfi_data')
    # iterate through the map and upsert the new values
    pg_hook = PostgresHook(
        postgres_conn_id='postgres',
        schema='veryfidb'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    doc_dict = docs[0]
    for key in doc_dict:
        document_id = key
        total_value = doc_dict[key]
        # create sql statement
        sql_stmt = "INSERT INTO total VALUES(" + str(document_id) + "," + str(total_value) + ") ON CONFLICT(document_id) DO UPDATE SET total = EXCLUDED.total"
        print(sql_stmt)
        cursor.execute(sql_stmt)
    pg_conn.commit()

with DAG(
    dag_id='postgres_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2023, month=1, day=1),
    catchup=False
) as dag:

    #1 create an output table
    task_create_output_table = PostgresOperator(
        task_id="create_output_table",
        postgres_conn_id="postgres",
        sql="""
            CREATE TABLE IF NOT EXISTS total (
            document_id int PRIMARY KEY,
            total VARCHAR
            );"""
    )


    # 2. Get the data from the documents dable table in veryfi database
    task_get_veryfi_data = PythonOperator(
        task_id='get_veryfi_data',
        python_callable=get_veryfi_data,
        do_xcom_push=True
    )

     # 3. Process the verify data
    task_process_veryfi_data = PythonOperator(
        task_id='process_veryfi_data',
        python_callable=process_veryfi_data
    )

    # 4. Write to the output table
    task_write_veryfi_data = PythonOperator(
        task_id='write_veryfi_data',
        python_callable=write_veryfi_data
    )
    
task_create_output_table >> task_get_veryfi_data >> task_process_veryfi_data >> task_write_veryfi_data
