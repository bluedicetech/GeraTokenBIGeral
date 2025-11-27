from fastapi import FastAPI
from dotenv import load_dotenv
import os
from query import *
import pandas as pd
import subprocess
from apscheduler.schedulers.background import BackgroundScheduler
import schedule
import threading
from functools import partial


load_dotenv()

global status

app = FastAPI()

# Para rodar no windows 
def get_powerbi_access_token(username, password, banco_empresa):
    cmd_debian = 'pwsh'
    cmd_windows = 'powershell'
    try:
        print(f"Executando rotina para {banco_empresa} às {datetime.now()}")
        
        # Cria o script PowerShell que fará login e pegará o token
        powershell_script = f"""
        $password = ConvertTo-SecureString "{password}" -AsPlainText -Force
        $credential = New-Object System.Management.Automation.PSCredential ("{username}", $password)
        Login-PowerBI -Credential $credential
        $token = Get-PowerBIAccessToken -AsString
        $token
        """
        
        # Executa o script PowerShell
        result = subprocess.run([cmd_debian, "-Command", powershell_script], capture_output=True, text=True)
        
        # Verifica se o comando foi bem-sucedido
        if result.returncode == 0:
            token = result.stdout.strip()
            print(f"Token bruto recebido: {token}")  # Debug
            
            parts = token.split("Bearer ")
            if len(parts) > 1:
                token = parts[1].strip()
                print(f"Token processado: {token}")  # Debug
                
                try:
                    inserir_chave_banco(token, banco_empresa)
                    print(f'Empresa: {banco_empresa} - Token gerado e inserido com sucesso!')
                except Exception as e:
                    print(f'Erro ao inserir token no banco para {banco_empresa}. Erro: {str(e)}')
            else:
                print(f"Formato de token inesperado para {banco_empresa}. Token completo: {token}")
                
        else:
            print(f"Erro ao executar o PowerShell script para {banco_empresa}: {result.stderr}")
        
        print(f'Rotina executada para {banco_empresa}')
        
    except Exception as e:
        print(f"Erro geral ao executar a rotina para {banco_empresa}: {str(e)}")
        import traceback
        traceback.print_exc()  # Isso mostrará o stack trace completo

    return status


def gera_dados_por_empresa():
    lista_banco_de_dados = []
    lista_empresas = []
    lista_banco_de_dados = consultar_banco_dados("bluedice")
    df_lista_banco_de_dados = pd.DataFrame(lista_banco_de_dados)
    print(df_lista_banco_de_dados)
    for index, row in df_lista_banco_de_dados.iterrows():
        nome_empresa = 'emp_' + str(row['cnpj']).replace('/','').replace('.','').replace('-','')
        login = row['email_publicacao']
        senha = "@Azul2512"
        lista_empresas.append({
        "empresa": nome_empresa,
        "login": login,
        "senha": senha
    })

    df_informacoes_login = pd.DataFrame(lista_empresas)
    return df_informacoes_login


def iniciar_agendador_simplificado():
    df = gera_dados_por_empresa()  
    
    for i in range(5, 23):
        time_str = f"{i:02d}:47"
        for index, empresa in df.iterrows():
            username = empresa['login']
            senha = empresa['senha']
            banco_empresa = empresa['empresa']
            
            # Criar uma função específica para cada empresa
            def criar_job(username, senha, banco_empresa):
                def job():
                    return get_powerbi_access_token(username, senha, banco_empresa)
                return job
            
            job = criar_job(username, senha, banco_empresa)
            schedule.every().day.at(time_str).do(job)
            print(f"Agendado: {banco_empresa} para {time_str}")
    
    print("Agendador simplificado iniciado")
    while True:
        schedule.run_pending()
        threading.Event().wait(60)

thread_agendador = threading.Thread(target=iniciar_agendador_simplificado, daemon=False)
thread_agendador.start()
thread_agendador.join()

        
