from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware
from io import StringIO
from fastapi.responses import StreamingResponse

import csv
import os
import tempfile


from database.get_data import get_data
from database.db_config import connection_and_cursor
from database.queries import queryMaisFuncionarios, queryEmpresaMaisAntiga, queryRegiaoMaisIndustrial, queryNumeroEmpresasPorSetor, queryTotalFuncionarios
from database.insert_into_neon import insert_data

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/api/upload/")
async def upload_file(csv_file: UploadFile = File(...)):
    try:
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            contents = await csv_file.read()
            temp_file.write(contents)
            temp_file_path = temp_file.name

        insert_data(temp_file_path)

        os.unlink(temp_file_path)

        mais_funcionarios = await queryMaisFuncionarios()
        empresa_mais_antiga = await queryEmpresaMaisAntiga()
        regiao_mais_industrial = await queryRegiaoMaisIndustrial()
        numero_empresas_por_setor = await queryNumeroEmpresasPorSetor()
        total_funcionarios = await queryTotalFuncionarios()

        return {"queryResults": {
            "maisFuncionarios": mais_funcionarios,
            "empresaMaisAntiga": empresa_mais_antiga,
            "regiaoMaisIndustrial": regiao_mais_industrial,
            "numeroEmpresasPorSetor": numero_empresas_por_setor,
            "totalFuncionarios": total_funcionarios
        }
        }

    except Exception as e:
        print('Erro ao processar o arquivo CSV:', e)
        raise HTTPException(status_code=500, detail='Erro ao processar o arquivo CSV')

@app.get("/api/download-csv")
async def download_csv():
    data = get_data()
    headers = ["nome","data_fundacao","num_funcionarios","regiao_brasil","setor_atuacao"] 

    csv_data = StringIO()
    csv_writer = csv.writer(csv_data)
    csv_writer.writerow(headers)
    for row in data:
        print(row)
        csv_writer.writerow(row)

    csv_data.seek(0)
    return StreamingResponse(iter([csv_data.getvalue()]), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=data.csv;", "Access-Control-Allow-Origin":"*"})

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)