import os
import psycopg

def get_connection():
    return psycopg.connect(
        user=os.getenv("PEA_DB_USER"),
        password=os.getenv("PEA_DB_PASSWORD"),
        host=os.getenv("PEA_DB_HOST"),
        port=os.getenv("PEA_DB_PORT"),
        dbname="pea_db"
    )
