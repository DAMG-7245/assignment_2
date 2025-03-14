from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime, timedelta
import os

# --------------------------------
# 默认参数设置
# --------------------------------
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# 定义 DAG
dag = DAG(
    'json_pipeline',
    default_args=default_args,
    description='JSON Financial Data Pipeline DAG',
    schedule_interval='@daily',
    catchup=False
)

# --------------------------------
# 1. 演示使用上下文参数的 Python 任务
# --------------------------------
def print_context(**context):
    """Shows how to use context variables in Airflow"""
    print(f"Execution date is {context['ds']}")
    print(f"Task instance: {context['task_instance']}")
    return "Hello from first Json task!"

task1 = PythonOperator(
    task_id='print_context',
    python_callable=print_context,
    dag=dag
)

# --------------------------------
# 2. 解压文件
# --------------------------------
def unzip_data(**context):
    os.system("unzip -o ../../2021q4/2021q4.zip -d ../../2021q4/extracted/")
    print("✅ Data unzipped successfully")

unzip_task = PythonOperator(
    task_id='unzip_sec_data',
    python_callable=unzip_data,
    dag=dag
)

# --------------------------------
# 3. 转换 CSV -> JSON
# --------------------------------
def transform_to_json(**context):
    # 假设 data_ingestion/sec_csv_reader.py 存在并执行转换逻辑
    os.system("python data_ingestion/sec_csv_reader.py")
    print("✅ JSON transformation complete")

transform_task = PythonOperator(
    task_id='transform_to_json',
    python_callable=transform_to_json,
    dag=dag
)

# --------------------------------
# 4. 创建 JSON_SCHEMA
# --------------------------------
create_schema = SnowflakeOperator(
    task_id='create_json_schema',
    snowflake_conn_id='snowflake_default',
    sql="""
    -- 切换到 DBT_DB 数据库（若没建好，需要先建数据库）
    USE DATABASE DBT_DB;
    -- 如果 JSON_SCHEMA 不存在则创建
    CREATE SCHEMA IF NOT EXISTS JSON_SCHEMA;
    """,
    dag=dag
)

# --------------------------------
# 5. 在 JSON_SCHEMA 下创建表
# --------------------------------
create_table = SnowflakeOperator(
    task_id='create_json_table',
    snowflake_conn_id='snowflake_default',
    sql="""
    USE SCHEMA DBT_DB.JSON_SCHEMA;
    CREATE TABLE IF NOT EXISTS json_sec_data (
        cik STRING,
        company_name STRING,
        filing_date DATE,
        fiscal_year INT,
        adsh STRING,
        tag STRING,
        value FLOAT,
        unit STRING,
        data VARIANT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag
)

# --------------------------------
# 6. 加载 JSON 数据到 Snowflake
# --------------------------------
def load_to_snowflake(**context):
    # 假设 storage/snowflake_loader_json.py 中执行 COPY INTO 或类似逻辑
    os.system("python storage/snowflake_loader_json.py")
    print("✅ JSON loaded into Snowflake")

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_to_snowflake,
    dag=dag
)

# --------------------------------
# 7. 运行 dbt models
# --------------------------------
run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="""
        echo "👉 Running dbt..."
        dbt run --profile data_pipeline --project-dir /opt/airflow/dbt/data_pipeline
    """,
    # 关键修正：通过 env 参数传递环境变量
    env={'DBT_PROFILES_DIR': '/opt/airflow/dbt/data_pipeline'},
    dag=dag
)

# --------------------------------
# 8. 运行 dbt tests
# --------------------------------
test_dbt = BashOperator(
    task_id='test_dbt_models',
    bash_command="echo '👉 Testing dbt models...' && dbt test --profile data_pipeline --project-dir /opt/airflow/dbt/data_pipeline",
    
    env={'DBT_PROFILES_DIR': '/opt/airflow/dbt/data_pipeline'},  
    dag=dag
)


# --------------------------------
# 设置任务依赖关系
# --------------------------------
# 可根据需要改变先后顺序，以下仅是示例
task1 >> unzip_task >> transform_task 
transform_task >> create_schema >> create_table >> load_task >> run_dbt >> test_dbt
