
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def credenciais_banco(database_empresa):
    conn = psycopg2.connect(
        host=os.getenv("Server"),
        database= database_empresa,
        user=os.getenv("UID"),
        password=os.getenv("PWD")
    )
    return conn

def consulta_credenciais_power_bi(empresa):
    conn = credenciais_banco()
    cursor = conn.cursor()
    query = f"SELECT username, password FROM {empresa} LIMIT 1"
    cursor.execute(query)
    result = cursor.fetchone()
    conn.close()
    if result:
        return result[0], result[1]
    else:
        return None, None