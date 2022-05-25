from utils.df_handle import *
from google_service import get_service
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='Gdocs'
prefix='TTTT_INS'
csv_path = '/usr/local/airflow/plugins'+'/'

dag_params = {
    'owner': 'nhanvo',
    "depends_on_past": False,
    'start_date': datetime(2021, 10, 1, tzinfo=local
    'email_on_failure': True,
    'email_on_retry': False,
    'email':['duyvq@merapgroup.com', 'vanquangduy10@
    'do_xcom_push': False,
    'execution_timeout':timedelta(seconds=300)
    'retries': 3,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(prefix+name,
          catchup=False,
          default_args=dag_params,
          https://crontab.guru/
          @once
          schedule_interval= '@once',
          tags=[prefix+name, 'Daily', 'at 02:05']
)
# Json_file ="D:/ipynb/sever_tttt_ins/datateam1599968716114-6f9f144b4262.json"

service = get_service()

spreadsheets_id = '1csSgQ4xamqJx5zbgJJAtSNhjTaLiAZtrAwppEFlzNUA'
rangeAll = '{0}!A:AA'.format('Check Sale Input')
body = {}

database = 'biteam'
host = '171.235.26.161'
username = 'biteam'
password = '123biteam'


conn = psycopg2.connect(
    host= host,
    database= database,
    user= username,
    password= password)
    
def run_sql():
    sql_template=''
    with io.open('D:/ipynb/sever_tttt_ins/thongtinthanhtoan.txt', "r", encoding="utf-8") as f:
         sql_template = f.read()#  .replace('\n', '')
    df = pd.read_sql_query(sql_template,conn)
    df1 =pd.DataFrame(df)
    df1['thoigiangoi'] = df1['thoigiangoi'].dt.strftime('%Y-%m-%d %H:%M:%S')
    return(df1)

def clear_data():
    resultClear = service.spreadsheets().values().clear( spreadsheetId=spreadsheets_id, range=rangeAll,body=body ).execute()

def insert_data():
    df1 = run_sql()
    response_date = service.spreadsheets().values().append(
        spreadsheetId=spreadsheets_id,
        valueInputOption='RAW',
        range='Check Sale Input!A1',
        body=dict(
            majorDimension='ROWS',
            values=df1.T.reset_index().T.values.tolist())
    ).execute()
    
def main():
    clear_data()
    insert_data()

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_insert_table = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_insert_table
# >> tab_refresh

