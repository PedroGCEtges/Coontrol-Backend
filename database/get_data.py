from database.db_config import connection_and_cursor

def get_data():
    try:
        connection, cursor = connection_and_cursor()

        exists_query = """ SELECT nome,data_fundacao,num_funcionarios,regiao_brasil,setor_atuacao FROM empresa_exemplo """
        cursor.execute(exists_query)
        return cursor.fetchall()
    except Exception as e:
        print('Erro ao inserir dados do CSV no banco de dados:', e)
        raise e

    finally:
        cursor.close()
        connection.close()