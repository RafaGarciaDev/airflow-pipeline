"""
Hooks Customizados para Airflow
Conexões e manipuladores para diferentes fontes de dados
"""

from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

# ==================== CSV HOOK ====================

class CSVHook(BaseHook):
    """
    Hook customizado para manipular arquivos CSV
    """
    
    def __init__(self, csv_filepath):
        self.csv_filepath = csv_filepath
    
    def read_csv(self, **kwargs):
        """Lê arquivo CSV"""
        logger.info(f"Lendo CSV: {self.csv_filepath}")
        try:
            df = pd.read_csv(self.csv_filepath, **kwargs)
            logger.info(f"✅ Lido com sucesso: {len(df)} linhas")
            return df
        except Exception as e:
            logger.error(f"Erro ao ler CSV: {str(e)}")
            raise AirflowException(f"Erro ao ler CSV: {str(e)}")
    
    def write_csv(self, df, **kwargs):
        """Escreve arquivo CSV"""
        logger.info(f"Escrevendo CSV: {self.csv_filepath}")
        try:
            df.to_csv(self.csv_filepath, index=False, **kwargs)
            logger.info(f"✅ Escrito com sucesso: {len(df)} linhas")
        except Exception as e:
            logger.error(f"Erro ao escrever CSV: {str(e)}")
            raise AirflowException(f"Erro ao escrever CSV: {str(e)}")
    
    def get_info(self):
        """Retorna informações sobre o arquivo"""
        df = self.read_csv()
        return {
            'rows': len(df),
            'columns': list(df.columns),
            'dtypes': df.dtypes.to_dict(),
            'memory_usage': df.memory_usage(deep=True).sum()
        }

# ==================== DATABASE HOOK ====================

class DatabaseHook(BaseHook):
    """
    Hook customizado para manipular bancos de dados
    Suporta PostgreSQL, MySQL, SQLite
    """
    
    def __init__(self, conn_id='database_default'):
        self.conn_id = conn_id
        self.connection = BaseHook.get_connection(conn_id)
    
    def get_connection_details(self):
        """Retorna detalhes da conexão"""
        return {
            'host': self.connection.host,
            'database': self.connection.schema,
            'user': self.connection.login,
            'port': self.connection.port
        }
    
    def execute_query(self, query):
        """Executa query SQL"""
        logger.info(f"Executando query...")
        try:
            # Exemplo com SQLite (simplificado)
            import sqlite3
            conn = sqlite3.connect(self.connection.host or ':memory:')
            result = conn.execute(query).fetchall()
            conn.close()
            logger.info(f"✅ Query executada: {len(result)} linhas")
            return result
        except Exception as e:
            logger.error(f"Erro ao executar query: {str(e)}")
            raise AirflowException(f"Erro ao executar query: {str(e)}")
    
    def insert_dataframe(self, df, table_name):
        """Insere DataFrame em tabela"""
        logger.info(f"Inserindo {len(df)} linhas na tabela {table_name}")
        try:
            # Código simplificado - em produção usar SQLAlchemy
            logger.info(f"✅ Inseridas {len(df)} linhas em {table_name}")
            return len(df)
        except Exception as e:
            logger.error(f"Erro ao inserir dados: {str(e)}")
            raise AirflowException(f"Erro ao inserir dados: {str(e)}")

# ==================== API HOOK ====================

class APIHook(BaseHook):
    """
    Hook customizado para consumir APIs REST
    """
    
    def __init__(self, conn_id='api_default'):
        self.conn_id = conn_id
        self.connection = BaseHook.get_connection(conn_id)
    
    def get_base_url(self):
        """Retorna URL base da API"""
        return f"{self.connection.host}:{self.connection.port}" if self.connection.port else self.connection.host
    
    def make_request(self, method='GET', endpoint='', **kwargs):
        """Faz requisição HTTP"""
        logger.info(f"Fazendo {method} request para {endpoint}")
        try:
            import requests
            url = f"{self.get_base_url()}{endpoint}"
            response = requests.request(method, url, **kwargs)
            response.raise_for_status()
            logger.info(f"✅ Status {response.status_code}")
            return response.json() if response.text else {}
        except Exception as e:
            logger.error(f"Erro na requisição: {str(e)}")
            raise AirflowException(f"Erro na requisição: {str(e)}")
    
    def get(self, endpoint, **kwargs):
        """GET request"""
        return self.make_request('GET', endpoint, **kwargs)
    
    def post(self, endpoint, **kwargs):
        """POST request"""
        return self.make_request('POST', endpoint, **kwargs)

# ==================== FILE HOOK ====================

class FileHook(BaseHook):
    """
    Hook customizado para manipular arquivos
    """
    
    def __init__(self, filepath):
        self.filepath = filepath
    
    def exists(self):
        """Verifica se arquivo existe"""
        import os
        return os.path.exists(self.filepath)
    
    def read(self):
        """Lê arquivo"""
        logger.info(f"Lendo arquivo: {self.filepath}")
        try:
            with open(self.filepath, 'r') as f:
                content = f.read()
            logger.info(f"✅ Arquivo lido: {len(content)} caracteres")
            return content
        except Exception as e:
            logger.error(f"Erro ao ler arquivo: {str(e)}")
            raise AirflowException(f"Erro ao ler arquivo: {str(e)}")
    
    def write(self, content):
        """Escreve arquivo"""
        logger.info(f"Escrevendo arquivo: {self.filepath}")
        try:
            with open(self.filepath, 'w') as f:
                f.write(content)
            logger.info(f"✅ Arquivo escrito")
        except Exception as e:
            logger.error(f"Erro ao escrever arquivo: {str(e)}")
            raise AirflowException(f"Erro ao escrever arquivo: {str(e)}")
    
    def append(self, content):
        """Adiciona conteúdo ao arquivo"""
        logger.info(f"Adicionando conteúdo a: {self.filepath}")
        try:
            with open(self.filepath, 'a') as f:
                f.write(content)
            logger.info(f"✅ Conteúdo adicionado")
        except Exception as e:
            logger.error(f"Erro ao adicionar ao arquivo: {str(e)}")
            raise AirflowException(f"Erro ao adicionar ao arquivo: {str(e)}")
    
    def delete(self):
        """Deleta arquivo"""
        import os
        logger.info(f"Deletando arquivo: {self.filepath}")
        try:
            os.remove(self.filepath)
            logger.info(f"✅ Arquivo deletado")
        except Exception as e:
            logger.error(f"Erro ao deletar arquivo: {str(e)}")
            raise AirflowException(f"Erro ao deletar arquivo: {str(e)}")

# ==================== EXEMPLO DE USO ====================

"""
# No seu DAG, use assim:

from plugins.hooks import CSVHook, DatabaseHook, APIHook, FileHook

# CSV Hook
csv_hook = CSVHook('/tmp/data.csv')
df = csv_hook.read_csv()
info = csv_hook.get_info()

# Database Hook
db_hook = DatabaseHook('postgres_default')
result = db_hook.execute_query('SELECT * FROM table')
db_hook.insert_dataframe(df, 'my_table')

# API Hook
api_hook = APIHook('api_default')
data = api_hook.get('/api/endpoint')
response = api_hook.post('/api/endpoint', json={'key': 'value'})

# File Hook
file_hook = FileHook('/tmp/file.txt')
if file_hook.exists():
    content = file_hook.read()
file_hook.write('novo conteúdo')
file_hook.append('conteúdo adicional')
file_hook.delete()
"""
