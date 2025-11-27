
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def credenciais_banco(database_empresa):
    Server = '157.230.88.244'
    UID = 'postgres'
    PWD = 'kBD6mBu-GenT$aF'
    conn = psycopg2.connect(
        host=Server,
        database= database_empresa,
        user=UID,
        password=PWD
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