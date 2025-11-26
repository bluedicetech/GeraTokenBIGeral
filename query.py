from credentials import *
import time
import pandas as pd
from datetime import datetime

def inserir_chave_banco(access_token_banco, banco_empresa):
    conn = credenciais_banco(banco_empresa)
    cursor = conn.cursor()
    data = datetime.now()
    for tentativa in range(5):
        try:
            cursor.execute("TRUNCATE TABLE API_TOKEN_POWER_BI")

            cursor.execute(
                "INSERT INTO API_TOKEN_POWER_BI (token, created_at, updated_at) VALUES (%s, %s, %s)",
                (access_token_banco,data,data)
            )

            conn.commit()
            print("Dados inseridos com sucesso!")
            return 

        except Exception as e:
            print(f"Falha na tentativa {tentativa + 1}: {e}")
            conn.rollback()  # garante que a transação seja revertida em caso de falha
            time.sleep(30)


def consultar_banco_dados(banco_empresa):
    print('chamando conexão com banco')
    conn = credenciais_banco(banco_empresa)
    df = pd.DataFrame()
    query = ''' SELECT * FROM empresas '''
    
    try:
        df = pd.read_sql(query,conn)
        conn.close()
        print('sucesso ao executar a consulta')
    except Exception as e:
        print("Erro ao executar a consulta:", e)

    return df