from utils.df_handle import *
import pendulum
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator


local_tz = pendulum.timezone("Asia/Bangkok")

name='DB'
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
    
# Sale input from GGform 
def clean_data_ggform():
    URL_ggform = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSYWdHL8hh3RoQTe4dAaidvJ83sbrKiZm9gseWgNru2uWRtfpKzYY87ix9rjl5AK5F7Fa3VGpGA4QgQ/pub?gid=1596587810&single=true&output=csv'
    df =pd.read_csv(URL_ggform, dayfirst=True, parse_dates=['Timestamp'])
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    df.rename({'ghichuvethongtinthanhtoan':'thongtinthanhtoan','tennhanvienbanhang':'tencvbh','timestamp':'thoigiangoi'}
          ,axis='columns',inplace =True)
    df['updated_at'] = datetime.now()
    df1=df[['manv','thongtinthanhtoan','giaiphap','thoigiangoi','makhcu','updated_at']]
    return df1

# Admin input
def clean_data_admininput():
    URL_admin ='https://docs.google.com/spreadsheets/d/e/2PACX-1vSYWdHL8hh3RoQTe4dAaidvJ83sbrKiZm9gseWgNru2uWRtfpKzYY87ix9rjl5AK5F7Fa3VGpGA4QgQ/pub?gid=206442339&single=true&output=csv'
    df = pd.read_csv(URL_admin,header = 0)
    df.columns = cleancols(df)
    df.columns = lower_col(df)
    # Dùng để mapping tuần sang ngày
    data_convert = pd.read_csv("D:/ipynb/sever_tttt_ins/convert_date.csv", dayfirst=True, parse_dates=['ngay'])
    reference = pd.DataFrame(data_convert)
    df_unpivoted = df.melt(id_vars=['makhcu','manv','tenkhachhang','tennhanvien','giaiphap'], 
                       var_name='tuan', value_name='thongtinthanhtoan')
    df3 = pd.DataFrame(df_unpivoted)
    df4=df3.merge(reference, on='tuan', how='left')
    df5=pd.DataFrame(df4)
    df5.rename({'ngay':'thoigiangoi'}
          ,axis='columns',inplace =True)
    df5['updated_at'] = datetime.now()
    df5['thoigiangoi'] = pd.to_datetime(df5['thoigiangoi'], format='%Y%m%d %H%M%S')
    df6=df5[['manv','thongtinthanhtoan','giaiphap','thoigiangoi','makhcu','updated_at']]
    df7=df6.dropna(subset=['manv','thongtinthanhtoan'])
    return df7

# Sale input form ggsheet

def clean_data_saleinput():
    URL_saleinput ='https://docs.google.com/spreadsheets/d/e/2PACX-1vSYWdHL8hh3RoQTe4dAaidvJ83sbrKiZm9gseWgNru2uWRtfpKzYY87ix9rjl5AK5F7Fa3VGpGA4QgQ/pub?gid=1035878545&single=true&output=csv'
    df = pd.read_csv(URL_saleinput,header = 0)
    report_links=df.Link
    index =0
    df2 = pd.DataFrame()
    for link in report_links:
        # print(index, link)
        # index += 1
        df1 = pd.read_csv(link)
        df1.columns = cleancols(df1)
        df1.columns = lower_col(df1)
        df2 =pd.concat([df2, df1],ignore_index=True).drop_duplicates()
    else:
        print('Đã hết số')
    # Dùng để mapping tuần sang ngày
    data_convert = pd.read_csv("D:/ipynb/sever_tttt_ins/convert_date.csv", dayfirst=True, parse_dates=['ngay'])
    reference = pd.DataFrame(data_convert)
    df_unpivoted = df2.melt(id_vars=['makhcu','manhanvien','tenkhachhang','tennhanvien','giaiphap'], 
                       var_name='tuan', value_name='thongtinthanhtoan')
    df3 = pd.DataFrame(df_unpivoted)
    df4=df3.merge(reference, on='tuan', how='left')
    df5=pd.DataFrame(df4)
    df5.rename({'ngay':'thoigiangoi','manhanvien':'manv'}
          ,axis='columns',inplace =True)
    df5['updated_at'] = datetime.now()
    df5['thoigiangoi'] = pd.to_datetime(df5['thoigiangoi'], format='%Y%m%d %H%M%S')
    df6=df5[['manv','thongtinthanhtoan','giaiphap','thoigiangoi','makhcu','updated_at']]
    df7=df6.dropna(subset=['manv','thongtinthanhtoan'])
    return df7

# UNION all data from another source

def union_data():
    df_sale = clean_data_saleinput()
    df_ggform =clean_data_ggform()
    df_admin = clean_data_admininput()
    df_all = pd.concat([df_admin, df_ggform,df_sale],ignore_index=True).drop_duplicates()
    return df_all

def update_table(df_all):
	df = df_all
	pk = ['manv','makhcu','thongtinthanhtoan']
	execute_values_upsert(df, "d_tttt_ins",pk)

def insert_bigquery():
    table_name ='d_tttt_ins'
    sql = \
    f"""
    Select * from {table_name}
    """
    df=get_ps_df(sql)
    pandas_gbq.to_gbq(df, f'biteam.{table_name}', project_id='spatial-vision-343005',if_exists='replace', table_schema = None)

def main(): 
    df_all = union_data() 
    update_table(df_all=df_all) #DB Postgres
    insert_bigquery() #DB Bigquery

if __name__ == "__main__":
    main()
    
dummy_start = DummyOperator(task_id="dummy_start", dag=dag)

py_insert_table = PythonOperator(task_id="main", python_callable=main, dag=dag)

dummy_start >> py_insert_table
