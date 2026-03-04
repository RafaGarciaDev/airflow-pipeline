"""
Operadores Customizados para Airflow
Extensões de operadores para tarefas específicas
"""

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.exceptions import AirflowException
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ==================== CSV READER OPERATOR ====================

class CSVReaderOperator(BaseOperator):
    """
    Operador customizado para ler arquivos CSV
    
    Args:
        filepath (str): Caminho do arquivo CSV
        delimiter (str): Delimitador do CSV (padrão: ',')
    """
    
    template_fields = ['filepath']
    
    @apply_defaults
    def __init__(self, filepath, delimiter=',', **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.delimiter = delimiter
    
    def execute(self, context):
        logger.info(f"Lendo arquivo CSV: {self.filepath}")
        
        try:
            df = pd.read_csv(self.filepath, delimiter=self.delimiter)
            logger.info(f"✅ Arquivo lido com sucesso: {len(df)} linhas, {len(df.columns)} colunas")
            
            # Enviar para XCom
            context['task_instance'].xcom_push(key='dataframe_info', value={
                'rows': len(df),
                'columns': list(df.columns)
            })
            
            return f"Lido: {len(df)} linhas"
        
        except Exception as e:
            logger.error(f"Erro ao ler CSV: {str(e)}")
            raise AirflowException(f"Falha ao ler CSV: {str(e)}")

# ==================== DATA TRANSFORMER OPERATOR ====================

class DataTransformerOperator(BaseOperator):
    """
    Operador customizado para transformar dados
    
    Args:
        input_file (str): Arquivo de entrada
        output_file (str): Arquivo de saída
        transformations (dict): Dicionário com transformações
    """
    
    template_fields = ['input_file', 'output_file']
    
    @apply_defaults
    def __init__(self, input_file, output_file, transformations=None, **kwargs):
        super().__init__(**kwargs)
        self.input_file = input_file
        self.output_file = output_file
        self.transformations = transformations or {}
    
    def execute(self, context):
        logger.info(f"Transformando dados de {self.input_file}")
        
        try:
            df = pd.read_csv(self.input_file)
            
            # Aplicar transformações
            for col, func in self.transformations.items():
                if col in df.columns:
                    logger.info(f"Aplicando transformação em {col}")
                    df[col] = df[col].apply(func)
            
            # Salvar resultado
            df.to_csv(self.output_file, index=False)
            logger.info(f"✅ Dados transformados e salvos em {self.output_file}")
            
            return f"Transformados: {len(df)} linhas"
        
        except Exception as e:
            logger.error(f"Erro na transformação: {str(e)}")
            raise AirflowException(f"Falha na transformação: {str(e)}")

# ==================== DATA VALIDATOR OPERATOR ====================

class DataValidatorOperator(BaseOperator):
    """
    Operador customizado para validar dados
    
    Args:
        filepath (str): Arquivo a validar
        rules (dict): Dicionário com regras de validação
    """
    
    template_fields = ['filepath']
    
    @apply_defaults
    def __init__(self, filepath, rules=None, **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.rules = rules or {}
    
    def execute(self, context):
        logger.info(f"Validando dados em {self.filepath}")
        
        try:
            df = pd.read_csv(self.filepath)
            errors = []
            
            # Validar se arquivo vazio
            if len(df) == 0:
                raise AirflowException("Arquivo vazio!")
            
            # Validar colunas obrigatórias
            required_columns = self.rules.get('required_columns', [])
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                errors.append(f"Colunas faltando: {missing_cols}")
            
            # Validar sem nulos
            if self.rules.get('no_nulls'):
                null_cols = df.columns[df.isnull().any()].tolist()
                if null_cols:
                    errors.append(f"Colunas com valores nulos: {null_cols}")
            
            # Validar sem duplicatas
            if self.rules.get('no_duplicates'):
                dup_count = df.duplicated().sum()
                if dup_count > 0:
                    errors.append(f"Encontradas {dup_count} linhas duplicadas")
            
            if errors:
                error_msg = "; ".join(errors)
                logger.error(f"Validação falhou: {error_msg}")
                raise AirflowException(f"Validação falhou: {error_msg}")
            
            logger.info("✅ Validação passou!")
            context['task_instance'].xcom_push(key='validation_status', value='PASSED')
            
            return "Validação OK"
        
        except Exception as e:
            logger.error(f"Erro na validação: {str(e)}")
            raise AirflowException(f"Erro na validação: {str(e)}")

# ==================== DATA LOADER OPERATOR ====================

class DataLoaderOperator(BaseOperator):
    """
    Operador customizado para carregar dados em banco de dados
    
    Args:
        filepath (str): Arquivo com dados
        table_name (str): Nome da tabela
        connection_id (str): ID da conexão no Airflow
    """
    
    template_fields = ['filepath', 'table_name']
    
    @apply_defaults
    def __init__(self, filepath, table_name, connection_id='postgres_default', **kwargs):
        super().__init__(**kwargs)
        self.filepath = filepath
        self.table_name = table_name
        self.connection_id = connection_id
    
    def execute(self, context):
        logger.info(f"Carregando dados de {self.filepath} para tabela {self.table_name}")
        
        try:
            df = pd.read_csv(self.filepath)
            
            # Aqui entraria código real de conexão com DB
            # from airflow.hooks.postgres_hook import PostgresHook
            # hook = PostgresHook(postgres_conn_id=self.connection_id)
            # hook.insert_rows(table=self.table_name, rows=df.values.tolist())
            
            logger.info(f"✅ Carregadas {len(df)} linhas na tabela {self.table_name}")
            
            context['task_instance'].xcom_push(key='loaded_rows', value=len(df))
            
            return f"Carregadas {len(df)} linhas"
        
        except Exception as e:
            logger.error(f"Erro ao carregar dados: {str(e)}")
            raise AirflowException(f"Falha ao carregar dados: {str(e)}")

# ==================== EXEMPLO DE USO ====================

"""
# No seu DAG, use assim:

from operators.custom_operators import (
    CSVReaderOperator,
    DataTransformerOperator,
    DataValidatorOperator,
    DataLoaderOperator
)

# Ler CSV
read_csv = CSVReaderOperator(
    task_id='read_csv',
    filepath='/tmp/input.csv',
    delimiter=','
)

# Validar
validate = DataValidatorOperator(
    task_id='validate',
    filepath='/tmp/input.csv',
    rules={
        'required_columns': ['id', 'name'],
        'no_nulls': True,
        'no_duplicates': False
    }
)

# Transformar
transform = DataTransformerOperator(
    task_id='transform',
    input_file='/tmp/input.csv',
    output_file='/tmp/output.csv',
    transformations={
        'name': lambda x: x.upper(),
        'value': lambda x: x * 2
    }
)

# Carregar
load = DataLoaderOperator(
    task_id='load',
    filepath='/tmp/output.csv',
    table_name='my_table',
    connection_id='postgres_default'
)

read_csv >> validate >> transform >> load
"""
