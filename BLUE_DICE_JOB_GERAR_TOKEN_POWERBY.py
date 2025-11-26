from __future__ import annotations
import logging
import os
import time
import subprocess
from datetime import datetime, timedelta
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2 import pool
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

DAG_ID = "BLUE_DICE_JOB_GERAR_TOKEN_GERAL_POWERBI"

# Configurar logging mais detalhado
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Pool de conexões global
connection_pool = None

def get_connection_pool():
    """Inicializa o pool de conexões se não existir"""
    global connection_pool
    if connection_pool is None:
        try:
            connection_pool = psycopg2.pool.SimpleConnectionPool(
                1, 10,
                host='157.230.88.244',
                database='bluedice',
                user='postgres',
                password='kBD6mBu-GenT$aF'
            )
            logger.info("Pool de conexões inicializado com sucesso")
        except Exception as e:
            logger.error(f"Erro ao inicializar pool de conexões: {e}")
            raise
    return connection_pool

@contextmanager
def get_db_connection(database_empresa: str = "bluedice"):
    """
    Context manager para gerenciar conexões com o banco de forma segura.
    """
    conn = None
    try:
        if database_empresa == "bluedice":
            pool = get_connection_pool()
            conn = pool.getconn()
            logger.debug("Conexão obtida do pool")
        else:
            conn = psycopg2.connect(
                host='157.230.88.244',
                database=database_empresa,
                user='postgres',
                password='kBD6mBu-GenT$aF'
            )
            logger.debug(f"Conexão direta criada para {database_empresa}")
        
        yield conn
    except Exception as e:
        logger.error(f"Erro ao obter conexão: {e}")
        raise
    finally:
        if conn:
            if database_empresa == "bluedice":
                pool = get_connection_pool()
                pool.putconn(conn)
                logger.debug("Conexão devolvida ao pool")
            else:
                conn.close()
                logger.debug("Conexão direta fechada")

def inserir_chave_banco(access_token_banco, banco_empresa):
    """
    Insere o token no banco com gerenciamento seguro de conexão.
    """
    logger.info(f"Tentando inserir token no banco {banco_empresa}")
    
    for tentativa in range(3):
        try:
            with get_db_connection(banco_empresa) as conn:
                cursor = conn.cursor()
                
                logger.info(f"Tentativa {tentativa + 1} - Inserindo token no banco {banco_empresa}...")

                cursor.execute("TRUNCATE TABLE API_TOKEN_POWER_BI")

                cursor.execute(
                    "INSERT INTO API_TOKEN_POWER_BI (token) VALUES (%s)",
                    (access_token_banco,)
                )

                conn.commit()
                logger.info("✓ Dados inseridos com sucesso!")
                break

        except Exception as e:
            logger.error(f"✗ Falha na tentativa {tentativa + 1}: {e}")
            time.sleep(10)
    else:
        raise RuntimeError(f"Não foi possível inserir o token no banco {banco_empresa} após 3 tentativas.")

def consultar_banco_dados(banco_empresa):
    """Consulta o banco com gerenciamento seguro de conexão"""
    logger.info(f'Conectando ao banco de dados {banco_empresa}')
    df = pd.DataFrame()
    query = ''' SELECT * FROM empresas '''
    
    try:
        with get_db_connection(banco_empresa) as conn:
            df = pd.read_sql(query, conn)
            logger.info(f'✓ Sucesso ao executar a consulta. Retornou {len(df)} registros')
    except Exception as e:
        logger.error(f"✗ Erro ao executar a consulta: {e}")
        return pd.DataFrame()

    return df

def gera_dados_por_empresa():
    """
    Obtém dados das empresas do banco bluedice.
    """
    logger.info("Obtendo dados das empresas...")
    lista_empresas = []

    try:
        lista_banco_de_dados = consultar_banco_dados("bluedice")
        
        if lista_banco_de_dados.empty:
            logger.warning("Nenhum dado retornado da consulta ao banco")
            return pd.DataFrame(), ""

        df_lista_banco_de_dados = pd.DataFrame(lista_banco_de_dados)
        logger.info(f"Encontradas {len(df_lista_banco_de_dados)} empresas no banco")

        for index, row in df_lista_banco_de_dados.iterrows():
            nome_empresa = 'emp_' + str(row['cnpj']).replace('/', '').replace('.', '').replace('-', '')
            login = row['email_publicacao']
            
            # Verificar se a senha está disponível nas variáveis de ambiente
            senha = "@Azul2512"
            
            if not senha:
                logger.warning(f"Senha não encontrada nas variáveis de ambiente para {nome_empresa}")
                senha = ""
            
            lista_empresas.append({
                "empresa": nome_empresa,
                "login": login,
                "senha": senha
            })
            logger.info(f"Empresa {index + 1}: {nome_empresa} - Login: {login}")

        df_informacoes_login = pd.DataFrame(lista_empresas)
        logger.info(f"✓ Total de {len(df_informacoes_login)} empresas processadas")
        
        if not df_informacoes_login.empty:
            ultima_empresa = df_informacoes_login.iloc[-1]['empresa']
        else:
            ultima_empresa = ""
            
        return df_informacoes_login, ultima_empresa
        
    except Exception as e:
        logger.error(f"Erro em gera_dados_por_empresa: {e}")
        return pd.DataFrame(), ""

def install_powershell_linux():
    """
    Tenta instalar o PowerShell no Linux se não estiver disponível
    """
    logger.info("Verificando se PowerShell está disponível...")
    
    # Testar se pwsh (PowerShell Core) está disponível
    try:
        result = subprocess.run(["pwsh", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✓ PowerShell Core (pwsh) já está instalado")
            return "pwsh"
    except:
        pass
    
    # Testar se powershell está disponível
    try:
        result = subprocess.run(["powershell", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("✓ PowerShell já está instalado")
            return "powershell"
    except:
        pass
    
    logger.warning("PowerShell não encontrado. Tentando instalar...")
    
    # Tentar instalar PowerShell Core no Linux
    try:
        # Para Ubuntu/Debian
        if os.path.exists('/etc/debian_version'):
            logger.info("Instalando PowerShell Core no Debian/Ubuntu...")
            subprocess.run([
                "bash", "-c", 
                "wget -q https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb && "
                "sudo dpkg -i packages-microsoft-prod.deb && "
                "sudo apt-get update && "
                "sudo apt-get install -y powershell"
            ], check=True)
            return "pwsh"
        
        # Para CentOS/RHEL
        elif os.path.exists('/etc/redhat-release'):
            logger.info("Instalando PowerShell Core no CentOS/RHEL...")
            subprocess.run([
                "bash", "-c",
                "curl https://packages.microsoft.com/config/rhel/7/prod.repo | sudo tee /etc/yum.repos.d/microsoft.repo && "
                "sudo yum install -y powershell"
            ], check=True)
            return "pwsh"
        
        else:
            logger.error("Sistema operacional não suportado para instalação automática do PowerShell")
            return None
            
    except Exception as e:
        logger.error(f"Falha na instalação do PowerShell: {e}")
        return None

def install_powerbi_module(powershell_cmd):
    """
    Instala o módulo Power BI se não estiver instalado
    """
    logger.info("Verificando módulo Power BI...")
    
    check_script = """
    try {
        Get-Module -Name MicrosoftPowerBIMgmt -ListAvailable
        Write-Output "MODULO_INSTALADO"
    } catch {
        Write-Output "MODULO_NAO_INSTALADO"
    }
    """
    
    try:
        result = subprocess.run(
            [powershell_cmd, "-Command", check_script], 
            capture_output=True, 
            text=True,
            timeout=30
        )
        
        if "MODULO_INSTALADO" in result.stdout:
            logger.info("✓ Módulo Power BI está instalado")
            return True
        else:
            logger.info("Instalando módulo Power BI...")
            install_script = "Install-Module -Name MicrosoftPowerBIMgmt -Force -AcceptLicense -AllowClobber"
            result = subprocess.run(
                [powershell_cmd, "-Command", install_script],
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode == 0:
                logger.info("✓ Módulo Power BI instalado com sucesso")
                return True
            else:
                logger.error(f"✗ Falha ao instalar módulo Power BI: {result.stderr}")
                return False
                
    except Exception as e:
        logger.error(f"Erro ao verificar/instalar módulo Power BI: {e}")
        return False

def get_powerbi_access_token(username, password, banco_empresa):
    """
    Obtém token de acesso do Power BI usando PowerShell.
    Versão corrigida com escape adequado.
    """
    logger.info(f"=== INICIANDO OBTENÇÃO DE TOKEN ===")
    logger.info(f"Usuário: {username}")
    logger.info(f"Banco: {banco_empresa}")
    logger.info(f"Senha disponível: {'Sim' if password else 'Não'}")
    
    if not password:
        logger.error("Senha não fornecida!")
        return None

    try:
        # Criar um arquivo temporário com o script PowerShell
        # Isso evita problemas de escape de caracteres
        script_content = f'''
$ErrorActionPreference = "Stop"

try {{
    Write-Host "[-] Importando módulo Power BI..."
    Import-Module MicrosoftPowerBIMgmt -Force
    
    Write-Host "[-] Convertendo credenciais..."
    $securePassword = ConvertTo-SecureString "{password}" -AsPlainText -Force
    $credential = New-Object System.Management.Automation.PSCredential ("{username}", $securePassword)
    
    Write-Host "[-] Realizando login no Power BI..."
    $loginResult = Login-PowerBI -Credential $credential
    
    Write-Host "[-] Obtendo token de acesso..."
    $token = Get-PowerBIAccessToken -AsString
    
    Write-Host "[-] Token obtido com sucesso!"
    Write-Output $token
}}
catch {{
    Write-Host "[ERRO] $($_.Exception.Message)"
    Write-Error $_.Exception.Message
    exit 1
}}
'''
        
        # Salvar script em arquivo temporário
        script_path = "/tmp/powerbi_token_script.ps1"
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        logger.info(f"Script PowerShell salvo em: {script_path}")
        logger.info("Executando script PowerShell para obter token...")
        
        # Executar o script do arquivo
        result = subprocess.run(
            ["pwsh", "-File", script_path], 
            capture_output=True, 
            text=True,
            timeout=120
        )
        
        # Limpar arquivo temporário
        try:
            os.remove(script_path)
        except:
            pass
        
        logger.info(f"PowerShell finalizado - Código de saída: {result.returncode}")
        logger.info(f"STDOUT: {result.stdout}")
        if result.stderr:
            logger.info(f"STDERR: {result.stderr}")
        
        # Verificar resultado
        if result.returncode == 0 and result.stdout:
            token = result.stdout.strip()
            logger.info(f"Token obtido (tamanho: {len(token)} caracteres)")
            
            # Processar o token
            if "Bearer " in token:
                token = token.split("Bearer ")[1].strip()
                logger.info("Token processado (removido 'Bearer ')")
            
            # Inserir no banco
            try:
                inserir_chave_banco(token, banco_empresa)
                logger.info(f"✓ Token inserido com sucesso no banco {banco_empresa}")
                return token
            except Exception as e:
                logger.error(f"Erro ao inserir token no banco: {e}")
                return None
        else:
            logger.error(f"Falha ao obter token. Return code: {result.returncode}")
            if result.stderr:
                logger.error(f"STDERR: {result.stderr}")
            return None
            
    except subprocess.TimeoutExpired:
        logger.error("Timeout ao executar script PowerShell (120 segundos)")
        return None
    except Exception as e:
        logger.error(f"Erro inesperado: {e}")
        return None

def close_connection_pool():
    """Fecha o pool de conexões ao final da execução"""
    global connection_pool
    if connection_pool:
        connection_pool.closeall()
        logger.info("Pool de conexões fechado")

def iniciar_agendador():
    """
    Função principal que inicia o agendador de tokens.
    """
    logger.info("🚀 INICIANDO AGENDADOR DE GERAÇÃO DE TOKENS...")
    
    try:
        df, banco_empresa_ultima = gera_dados_por_empresa()  
        
        if df.empty:
            logger.warning("❌ Nenhuma empresa encontrada para processar!")
            return

        logger.info(f"📊 Total de empresas para processar: {len(df)}")

        # Processar cada empresa
        tokens_gerados = 0
        for index, empresa in df.iterrows():
            username = empresa['login']
            senha = empresa['senha']
            banco_empresa = empresa['empresa']

            logger.info(f"\n" + "="*50)
            logger.info(f"🔹 Processando empresa {index + 1}/{len(df)}")
            logger.info(f"🔹 Empresa: {banco_empresa}")
            logger.info(f"🔹 Login: {username}")
            
            token = get_powerbi_access_token(username, senha, banco_empresa)
            
            if token:
                tokens_gerados += 1
                logger.info(f"✅ SUCESSO: Token gerado para {banco_empresa}")
            else:
                logger.error(f"❌ FALHA: Não foi possível gerar token para {banco_empresa}")
                
        logger.info(f"🎯 PROCESSAMENTO CONCLUÍDO! {tokens_gerados}/{len(df)} tokens gerados com sucesso!")
        
    except Exception as e:
        logger.error(f"💥 ERRO CRÍTICO NO AGENDADOR: {e}")
        raise
    finally:
        close_connection_pool()

# --- Configuração da DAG ---

SCHEDULE = "0 5-23 * * *" 

default_args = {
    "owner": "Leidiane Beatriz",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    description="Gerar tokens do power bi para todas as empresas - BLUEDICE",
    default_args=default_args,
    schedule_interval=SCHEDULE,
    start_date=datetime(2025, 11, 25),
    catchup=False,
    tags=["BLUEDICE", "TOKEN", "POWERBI", "GERAL"],
) as dag:
    
    gerar_tokens_task = PythonOperator(
        task_id="iniciar_agendador",
        python_callable=iniciar_agendador,
    )
    
    gerar_tokens_task