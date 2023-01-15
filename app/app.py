import time
import random
import json

from sqlalchemy import create_engine

# This file simulates the receipt data generation and loading every 3 seconds.
# It generates and persists the data in the documents table
# Following parameters are for database details to connect to.

db_name = 'veryfidb'
db_user = 'veryfi'
db_pass = 'veryfi'
db_host = 'postgres'
db_port = '5432'

# Connect to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

def add_new_row(payload):
    # Insert a new payload into the 'documents' table.
    try:
        db.execute("INSERT INTO documents (ml_response) " + "VALUES (" +"'" + payload + "'"+ ");")
    except Exception as e:
        print("It seems the database is not created yet, try again")
        print(e)

def rand_paylod():
    payload = {}
    if random.randint(0, 10) > 5:
        payload = {
            "value" : f"{random.randint(0, 10000)}",
            "score" : round(random.random(), 2),
            "ocr_score" : round(random.random(), 2),
            "bounding_box" : [round(random.random(), 2) for i in range(4)]
        }
        return payload

while True:
    choices = [
        rand_paylod(),
        [rand_paylod() for i in range(0, 9)]
    ]

    payload = {"business_id" : random.randint(0, 9)}
    fields = ["total", "line_items"]
    for f in fields:
        payload[f] = choices[random.randint(0, 9) % len(fields)]
    # print(payload)
    # write this row to database
    json_str = json.dumps(payload)
    print(json_str)
    add_new_row(json_str)
    time.sleep(3)
