from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import pandas as pd
import sqlite3

# Definições padrão para as tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['your_email@indicium.tech'],  
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_orders():
    # Conectar ao banco de dados e extrair dados da tabela 'Order'
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    orders_df = pd.read_sql_query("SELECT * FROM 'Order'", conn)
    orders_df.to_csv('output_orders.csv', index=False)
    conn.close()

def calculate_quantity_rio():
    # Ler os dados da tabela 'OrderDetail' e do arquivo 'output_orders.csv'
    conn = sqlite3.connect('data/Northwind_small.sqlite')
    order_details_df = pd.read_sql_query("SELECT * FROM 'OrderDetail'", conn)
    conn.close()

    # Ler o arquivo CSV
    orders_df = pd.read_csv('output_orders.csv')

    # Realizar o JOIN e calcular a soma da quantidade vendida para o Rio de Janeiro
    merged_df = pd.merge(order_details_df, orders_df, left_on = 'OrderId', right_on = 'Id')
    total_quantity_rio = merged_df[merged_df['ShipCity'] == 'Rio de Janeiro']['Quantity'].sum()

    # Exportar o resultado em count.txt
    with open('count.txt', 'w') as f:
        f.write(str(total_quantity_rio))

# def process_orders():
#     conn = sqlite3.connect('data/Northwind_small.sqlite')
#     cursor = conn.cursor()

#     # Leitura do CSV não é necessária aqui, pois a query será feita diretamente no banco
#     query = """
#     SELECT SUM(OrderDetail.Quantity) FROM OrderDetail
#     JOIN 'Order' ON OrderDetail.OrderID = 'Order'.ID
#     WHERE 'Order'.ShipCity = 'Rio de Janeiro';
#     """
#     cursor.execute(query)
#     result = cursor.fetchone()

#     with open('airflow-data/dags/count.txt', 'w') as f:
#         f.write(str(result[0]))

#     conn.close()

def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None

# Criar o DAG
with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """

    # Task 1: Extrair os dados da tabela 'Order'
    extract_orders_task = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )

    # Task 2: Calcular a quantidade vendida para o Rio de Janeiro
    calculate_quantity_rio_task = PythonOperator(
        task_id='calculate_quantity_rio',
        python_callable=calculate_quantity_rio,
    )

    # Task Final: Exportar a saída final
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    # Definindo a ordem de execução das tasks
    extract_orders_task >> calculate_quantity_rio_task >> export_final_output
