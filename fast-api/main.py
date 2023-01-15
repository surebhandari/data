from fastapi import FastAPI
from sqlalchemy import create_engine
from fastapi import Response
import json

db_name = 'veryfidb'
db_user = 'veryfi'
db_pass = 'veryfi'
db_host = 'postgres'
db_port = '5432'

# Connect to the database
db_string = 'postgresql://{}:{}@{}:{}/{}'.format(db_user, db_pass, db_host, db_port, db_name)
db = create_engine(db_string)

app = FastAPI()

@app.get('/')
def index():
    return {'Hello' : 'Veryfi!!'}

@app.get("/api/business/{business_id}")
def get_total(business_id: int):
    try:
        rs = db.execute("select total from  total where document_id ="+str(business_id)+";")
        json_str = json.dumps(rs.first()[0], indent=4, default=str)
        return Response(content=json_str, media_type='application/json')
    except Exception as e:
        print("Oops we got an exception while reading the analytics data for the document_id = " + str(business_id))
        return e
