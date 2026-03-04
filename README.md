# 🛠️ Pipeline de Dados com Apache Airflow

Pipeline ETL completo com orquestração de tarefas usando Apache Airflow, incluindo validação de dados, monitoramento e alertas.

## ✨ Funcionalidades

- **DAGs**: Fluxos de trabalho parametrizados
- **Operadores Customizados**: Tarefas específicas
- **Sensores**: Monitoramento de dependências
- **XCom**: Comunicação entre tarefas
- **Retry & Alertas**: Tratamento de falhas
- **Logging Avançado**: Rastreamento completo

## 🛠️ Tecnologias

- **Apache Airflow**: Orquestração
- **Python**: Operadores customizados
- **PostgreSQL**: Backend
- **Redis**: Celery Executor

## 🚀 Como Executar

```bash
# Inicializar Airflow
airflow db init

# Criar usuário
airflow users create --username admin --password admin

# Iniciar webserver
airflow webserver --port 8080

# Iniciar scheduler em outro terminal
airflow scheduler

# Acessar UI
# http://localhost:8080
```

## 📁 Estrutura

```
airflow-pipeline/
├── dags/
│   ├── etl_pipeline.py      # DAG principal
│   └── data_validation.py   # DAG de validação
├── operators/
│   └── custom_operators.py  # Operadores customizados
├── plugins/
│   └── hooks.py             # Hooks customizados
├── requirements.txt
└── README.md
```

## 💡 Exemplo DAG

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1)
)

def extract():
    # Código de extração
    pass

def transform():
    # Código de transformação
    pass

task1 = PythonOperator(task_id='extract', python_callable=extract, dag=dag)
task2 = PythonOperator(task_id='transform', python_callable=transform, dag=dag)

task1 >> task2
```

## 📈 Características Avançadas

- SLA Monitoring
- Dynamic DAG Generation
- Backfill de dados
- Task Groups
- Branching Logic

## 📝 Licença

MIT License

---

⭐ Se este projeto foi útil, deixe uma star!
