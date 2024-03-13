import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("PGHOST")
db = os.getenv("PGDATABASE")
user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")

def connection_and_cursor():
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}/{db}?sslmode=require')
    cur = conn.cursor()
    return conn, cur

def createTable():
    try:
        conn, cur = connection_and_cursor()

        query = '''
        CREATE TABLE empresa_exemplo (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(255),
            regiao_brasil VARCHAR(255),
            setor_atuacao VARCHAR(255),
            num_funcionarios INT,
            data_fundacao DATE
        )
        '''
        cur.execute(query)
        conn.commit()

    except Exception as e:
        print(e)