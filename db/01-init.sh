#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $VERYFI_USER WITH PASSWORD '$VERYFI_PASSWORD';
  CREATE DATABASE $VERYFI_DB;
  GRANT ALL PRIVILEGES ON DATABASE $VERYFI_DB TO $VERYFI_USER;
  \connect $VERYFI_DB $VERYFI_USER
  BEGIN;
    CREATE TABLE IF NOT EXISTS documents (
            document_id SERIAL PRIMARY KEY,
            ml_response VARCHAR
	);
    CREATE TABLE IF NOT EXISTS tracker (
            batch_size INT 
        );
    INSERT INTO tracker VALUES(0);
  COMMIT;
EOSQL
