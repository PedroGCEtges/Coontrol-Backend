import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

host = os.getenv("PGHOST")
db = os.getenv("PGDATABASE")
user = os.getenv("PGUSER")
password = os.getenv("PGPASSWORD")

async def executeQuery(query):
  try:
    conn = psycopg2.connect(f'postgresql://{user}:{password}@{host}/{db}?sslmode=require')
    cursor = conn.cursor()
    cursor.execute(query)
    return cursor.fetchone()
  except Exception as e: 
    print('Erro ao executar consulta:', e);
  finally:
    cursor.close()
    conn.close()

async def queryMaisFuncionarios():
    query = '''
    SELECT regiao_brasil, SUM(num_funcionarios) AS total_funcionarios
    FROM empresa_importada
    GROUP BY regiao_brasil
    ORDER BY total_funcionarios DESC
    LIMIT 1
    '''
    return await executeQuery(query)

async def queryEmpresaMaisAntiga():
  query = '''
    SELECT nome AS Nome_Empresa, data_fundacao AS Data_Fundacao
    FROM empresa_importada
    ORDER BY data_fundacao ASC
    LIMIT 1
  '''
  return await executeQuery(query)


async def queryRegiaoMaisIndustrial():
  query = '''
    SELECT regiao_brasil AS Regiao, COUNT(*) AS Numero_Empresas_Industriais
    FROM empresa_importada
    WHERE setor_atuacao = 'Industrial'
    GROUP BY regiao_brasil
    ORDER BY Numero_Empresas_Industriais DESC
    LIMIT 1
  '''
  return await executeQuery(query)


async def queryNumeroEmpresasPorSetor():
  query = '''
    SELECT setor_atuacao AS Setor, COUNT(*) AS Numero_Empresas
    FROM empresa_importada
    GROUP BY setor_atuacao
    ORDER BY Numero_Empresas DESC'''

  return await executeQuery(query)


async def queryTotalFuncionarios():
  query = '''
    SELECT SUM(num_funcionarios) AS Total_Funcionarios
    FROM empresa_importada
  '''
  return await executeQuery(query)




