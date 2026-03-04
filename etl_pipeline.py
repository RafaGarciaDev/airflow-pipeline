"""
Pipeline ETL com Apache Airflow
Inclui extração, transformação, carregamento e validação de dados
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

# ==================== ARGUMENTOS DEFAULT ====================

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1)
}

# ==================== FUNÇÕES DE TAREFA ====================

def extract_data(**context):
    """Extrai dados da fonte"""
    logger.info("Iniciando extração de dados...")
    
    # Simular extração
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Carlos', 'Diana', 'Eva'],
        'value': [100, 150, 200, 175, 225]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('/tmp/raw_data.csv', index=False)
    
    logger.info(f"Extração concluída: {len(df)} linhas")
    context['task_instance'].xcom_push(key='extracted_rows', value=len(df))
    
    return "Extração bem-sucedida"

def validate_data(**context):
    """Valida dados extraídos"""
    logger.info("Iniciando validação de dados...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    # Validações
    assert not df.isnull().any().any(), "Dados contêm valores nulos"
    assert len(df) > 0, "Arquivo vazio"
    assert df['value'].min() > 0, "Valores negativos detectados"
    
    logger.info("✅ Validação passou")
    context['task_instance'].xcom_push(key='validation_status', value='success')

def transform_data(**context):
    """Transforma dados"""
    logger.info("Iniciando transformação...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    # Transformações
    df['value_squared'] = df['value'] ** 2
    df['name_upper'] = df['name'].str.upper()
    df['created_at'] = datetime.now().isoformat()
    
    df.to_csv('/tmp/transformed_data.csv', index=False)
    logger.info("Transformação concluída")

def load_data(**context):
    """Carrega dados no destino"""
    logger.info("Iniciando carregamento...")
    
    df = pd.read_csv('/tmp/transformed_data.csv')
    
    # Simular carregamento em banco de dados
    logger.info(f"Carregando {len(df)} linhas no banco de dados")
    
    # Aqui entraria código real de conexão com DB
    # exemplo: df.to_sql('table_name', engine, if_exists='append')
    
    logger.info("✅ Carregamento concluído")

def data_quality_check(**context):
    """Verifica qualidade dos dados carregados"""
    logger.info("Iniciando verificação de qualidade...")
    
    df = pd.read_csv('/tmp/transformed_data.csv')
    
    quality_metrics = {
        'total_rows': len(df),
        'null_count': df.isnull().sum().sum(),
        'duplicate_count': df.duplicated().sum(),
        'quality_score': (1 - (df.isnull().sum().sum() / (df.shape[0] * df.shape[1]))) * 100
    }
    
    logger.info(f"Métricas de qualidade: {quality_metrics}")
    
    if quality_metrics['quality_score'] < 90:
        raise AirflowException("Qualidade dos dados abaixo de 90%")
    
    context['task_instance'].xcom_push(key='quality_metrics', value=quality_metrics)

def generate_report(**context):
    """Gera relatório de execução"""
    logger.info("Gerando relatório...")
    
    task_instance = context['task_instance']
    extracted = task_instance.xcom_pull(task_ids='extract', key='extracted_rows')
    validation = task_instance.xcom_pull(task_ids='validate', key='validation_status')
    quality = task_instance.xcom_pull(task_ids='quality_check', key='quality_metrics')
    
    report = f"""
    ===== RELATÓRIO DE EXECUÇÃO =====
    Data: {datetime.now()}
    
    Extração: {extracted} linhas
    Validação: {validation}
    Qualidade: {quality['quality_score']:.2f}%
    Duplicatas: {quality['duplicate_count']}
    
    ✅ Pipeline executado com sucesso!
    """
    
    logger.info(report)

def branch_decision(**context):
    """Decide qual caminho seguir baseado em condições"""
    quality = context['task_instance'].xcom_pull(
        task_ids='quality_check', 
        key='quality_metrics'
    )
    
    if quality['quality_score'] >= 95:
        return 'high_quality_path'
    else:
        return 'standard_path'

# ==================== DAG DEFINITION ====================

with DAG(
    'etl_pipeline_completo',
    default_args=default_args,
    description='Pipeline ETL completo com validação e monitoramento',
    schedule_interval='@daily',  # Executa diariamente
    catchup=False,
    tags=['etl', 'data-engineering']
) as dag:
    
    # Tarefas iniciais
    start = DummyOperator(task_id='start')
    
    # Extração
    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True
    )
    
    # Validação
    validate = PythonOperator(
        task_id='validate',
        python_callable=validate_data,
        provide_context=True
    )
    
    # Transformação
    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True
    )
    
    # Carregamento
    load = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True
    )
    
    # Verificação de qualidade
    quality_check = PythonOperator(
        task_id='quality_check',
        python_callable=data_quality_check,
        provide_context=True
    )
    
    # Branch condicional
    branch = BranchPythonOperator(
        task_id='branch',
        python_callable=branch_decision,
        provide_context=True
    )
    
    # Caminhos alternativos
    high_quality_path = DummyOperator(task_id='high_quality_path')
    standard_path = DummyOperator(task_id='standard_path')
    
    # Geração de relatório
    report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
        trigger_rule='all_done'  # Executa mesmo se houver falhas
    )
    
    # Task final
    end = DummyOperator(
        task_id='end',
        trigger_rule='all_done'
    )
    
    # Definir dependências
    start >> extract >> validate >> transform >> load >> quality_check >> branch
    
    branch >> [high_quality_path, standard_path]
    
    [high_quality_path, standard_path] >> report >> end

# ==================== TASK GROUP EXEMPLO ====================

with DAG(
    'etl_pipeline_com_groups',
    default_args=default_args,
    description='Pipeline com Task Groups',
    schedule_interval='@weekly',
    tags=['etl']
) as dag2:
    
    start = DummyOperator(task_id='start')
    
    with TaskGroup('extraction_group') as extraction_group:
        extract1 = PythonOperator(task_id='extract_source1', python_callable=extract_data)
        extract2 = PythonOperator(task_id='extract_source2', python_callable=extract_data)
    
    with TaskGroup('processing_group') as processing_group:
        validate = PythonOperator(task_id='validate', python_callable=validate_data)
        transform = PythonOperator(task_id='transform', python_callable=transform_data)
    
    load = PythonOperator(task_id='load', python_callable=load_data)
    end = DummyOperator(task_id='end')
    
    start >> extraction_group >> processing_group >> load >> end
