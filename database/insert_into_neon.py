import psycopg2
import csv
from datetime import datetime
from database.db_config import connection_and_cursor

def insert_data(file_path):
    def process_row(row):
        data_obj = datetime.strptime(row['data_fundacao'], '%d/%m/%Y')
        data_formatada = data_obj.strftime('%Y-%m-%d')
        return {
                    'nome': row['nome'],
                    'data_fundacao': data_formatada,
                    'num_funcionarios':row['num_funcionarios'],
                    'regiao_brasil': row['regiao_brasil'],
                    'setor_atuacao': row['setor_atuacao']
                }
    try: 
        connection, cursor = connection_and_cursor()

        exists_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'public' 
                AND table_name = 'empresa_importada'
            )
        """
        cursor.execute(exists_query)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            cursor.execute('TRUNCATE TABLE empresa_importada')

        else:
            create_table_query = """
                CREATE TABLE empresa_importada (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(255),
                    regiao_brasil VARCHAR(255),
                    setor_atuacao VARCHAR(255),
                    num_funcionarios INT,
                    data_fundacao DATE
                )
            """
            cursor.execute(create_table_query)
        try:
            with open(file_path, newline='', encoding='iso-8859-1') as csvfile:
                csvreader = csv.DictReader(csvfile, delimiter=',')
                rows = [process_row(row) for row in csvreader]

        except Exception:
            with open(file_path, newline='', encoding='iso-8859-1') as csvfile:
                csvreader = csv.DictReader(csvfile, delimiter=';')
                rows = [process_row(row) for row in csvreader]

        for row in rows:
            insert_query = """
                INSERT INTO empresa_importada (nome, regiao_brasil, setor_atuacao, num_funcionarios, data_fundacao)
                VALUES (%s, %s, %s, %s, %s)
            """
            values = (
                row['nome'],
                row['regiao_brasil'],
                row['setor_atuacao'],
                int(row['num_funcionarios']),
                datetime.strptime(row['data_fundacao'], '%Y-%m-%d').date()
            )
            cursor.execute(insert_query, values)

        connection.commit()
        print('Dados inseridos com sucesso')

    except Exception as e:
        print('Erro ao inserir dados do CSV no banco de dados:', e)
        raise e

    finally:
        cursor.close()
        connection.close()