"""
Pipeline de Validação de Dados
DAG para validar qualidade dos dados em múltiplas etapas
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta
import pandas as pd
import logging

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'data-quality',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 1, 1)
}

# ==================== FUNÇÕES DE VALIDAÇÃO ====================

def check_schema(**context):
    """Valida schema dos dados"""
    logger.info("Verificando schema...")
    
    # Verificar se colunas obrigatórias existem
    required_columns = ['id', 'name', 'value']
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    missing_columns = set(required_columns) - set(df.columns)
    
    if missing_columns:
        raise ValueError(f"Colunas faltando: {missing_columns}")
    
    logger.info("✅ Schema válido")
    return "Schema OK"

def check_data_types(**context):
    """Valida tipos de dados"""
    logger.info("Verificando tipos de dados...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    # Verificações
    assert df['id'].dtype in ['int64', 'int32'], "ID deve ser inteiro"
    assert df['name'].dtype == 'object', "Name deve ser string"
    assert df['value'].dtype in ['int64', 'float64'], "Value deve ser numérico"
    
    logger.info("✅ Tipos de dados corretos")
    return "Data types OK"

def check_missing_values(**context):
    """Verifica valores faltando"""
    logger.info("Verificando valores faltando...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    null_counts = df.isnull().sum()
    
    if null_counts.any():
        logger.warning(f"Valores nulos encontrados:\n{null_counts}")
        # Decidir se é crítico ou apenas aviso
    else:
        logger.info("✅ Sem valores faltando")
    
    return f"Missing values check: {null_counts.sum()} nulos encontrados"

def check_duplicates(**context):
    """Verifica duplicatas"""
    logger.info("Verificando duplicatas...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    duplicate_count = df.duplicated().sum()
    
    if duplicate_count > 0:
        logger.warning(f"Encontradas {duplicate_count} linhas duplicadas")
    else:
        logger.info("✅ Sem duplicatas")
    
    return f"Duplicate check: {duplicate_count} duplicatas"

def check_outliers(**context):
    """Detecta outliers"""
    logger.info("Detectando outliers...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    # Usar IQR para detectar outliers
    Q1 = df['value'].quantile(0.25)
    Q3 = df['value'].quantile(0.75)
    IQR = Q3 - Q1
    
    outliers = df[(df['value'] < (Q1 - 1.5 * IQR)) | (df['value'] > (Q3 + 1.5 * IQR))]
    
    logger.info(f"Outliers detectados: {len(outliers)}")
    
    return f"Outlier check: {len(outliers)} outliers encontrados"

def check_statistics(**context):
    """Calcula estatísticas dos dados"""
    logger.info("Calculando estatísticas...")
    
    df = pd.read_csv('/tmp/raw_data.csv')
    
    stats = {
        'total_rows': len(df),
        'mean_value': df['value'].mean(),
        'std_value': df['value'].std(),
        'min_value': df['value'].min(),
        'max_value': df['value'].max()
    }
    
    logger.info(f"Estatísticas: {stats}")
    
    context['task_instance'].xcom_push(key='stats', value=stats)
    
    return f"Statistics: {stats}"

def generate_quality_report(**context):
    """Gera relatório de qualidade"""
    logger.info("Gerando relatório de qualidade...")
    
    task_instance = context['task_instance']
    stats = task_instance.xcom_pull(task_ids='check_statistics', key='stats')
    
    report = f"""
    ===== RELATÓRIO DE QUALIDADE =====
    Data: {datetime.now()}
    
    Total de Linhas: {stats['total_rows']}
    Valor Médio: {stats['mean_value']:.2f}
    Desvio Padrão: {stats['std_value']:.2f}
    Valor Mínimo: {stats['min_value']:.2f}
    Valor Máximo: {stats['max_value']:.2f}
    
    ✅ Todas as verificações foram executadas
    """
    
    logger.info(report)
    
    # Salvar relatório
    with open('/tmp/quality_report.txt', 'w') as f:
        f.write(report)
    
    return "Relatório gerado"

# ==================== DAG ====================

with DAG(
    'data_validation_pipeline',
    default_args=default_args,
    description='Pipeline completo de validação de dados',
    schedule_interval='@daily',
    catchup=False,
    tags=['validation', 'data-quality']
) as dag:
    
    start = DummyOperator(task_id='start')
    
    # Validações em paralelo
    schema_check = PythonOperator(
        task_id='check_schema',
        python_callable=check_schema,
        provide_context=True
    )
    
    types_check = PythonOperator(
        task_id='check_data_types',
        python_callable=check_data_types,
        provide_context=True
    )
    
    missing_check = PythonOperator(
        task_id='check_missing_values',
        python_callable=check_missing_values,
        provide_context=True
    )
    
    duplicates_check = PythonOperator(
        task_id='check_duplicates',
        python_callable=check_duplicates,
        provide_context=True
    )
    
    outliers_check = PythonOperator(
        task_id='check_outliers',
        python_callable=check_outliers,
        provide_context=True
    )
    
    # Cálculo de estatísticas
    statistics = PythonOperator(
        task_id='check_statistics',
        python_callable=check_statistics,
        provide_context=True
    )
    
    # Gerar relatório
    report = PythonOperator(
        task_id='generate_quality_report',
        python_callable=generate_quality_report,
        provide_context=True,
        trigger_rule='all_done'
    )
    
    end = DummyOperator(task_id='end', trigger_rule='all_done')
    
    # Definir fluxo
    start >> [schema_check, types_check, missing_check, duplicates_check, outliers_check]
    [schema_check, types_check, missing_check, duplicates_check, outliers_check] >> statistics
    statistics >> report >> end
