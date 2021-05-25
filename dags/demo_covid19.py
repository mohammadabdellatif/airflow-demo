
from datetime import timedelta
from datetime import datetime
from textwrap import dedent
import tempfile as tf
import os

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from sqlalchemy import create_engine
import psycopg2
import shutil

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG(
    'demo_covid_19',
    default_args=default_args,
    description='A DAG which retrieves COVID-19 data from https://github.com/CSSEGISandData/COVID-19/tree/master/csse_covid_19_data then import it to postgres',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    def prepare_days_to_fetch():
        from_date = Variable.get("from_date", default_var='9/1/2020')
        to_date = Variable.get("to_date", default_var='5/30/2021')
        print(f'Fetch data from {from_date} to {to_date}')
        dr = pd.date_range(start=from_date, end=to_date)
        List_of_days=[]
        for d in dr:
            List_of_days.append(d.strftime('%m-%d-%Y'))
        return List_of_days

    def get_df_by_date(Day):
        DF_i=None
        country = Variable.get("country", default_var = 'United Kingdom')
        province_state = Variable.get("province_state", default_var = 'England')
        try:
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            DF_day['Day']=Day
            cond=(DF_day.Country_Region==country)&(DF_day.Province_State==province_state)
            Selec_columns=['Day','Country_Region', 'Last_Update',
                  'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                  'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
            pass
        return DF_i

    def save_dataframe_to_temp(df):
        td = tf.mkdtemp()
        csv_path = f'{td}/data.csv'
        df.to_csv(csv_path)
        print(f'csv data saved to {csv_path}')
        return csv_path

    def prepare_dataframes(**kwargs):
        print('start to fetch datasets')
        ti = kwargs['ti']
        dates = ti.xcom_pull(task_ids='prepare_days_to_fetch')
        DF_all=[]
        count = len(dates)
        for d in dates:
            print(f'fetch dataset for {d}')
            df = get_df_by_date(d)
            DF_all.append(df)
            count = count - 1
            print(f'{count} is remaining')
        l = len(DF_all)
        print(f'total dataframes retrieved {l}')
        df=pd.concat(DF_all).reset_index(drop=True)
        # Create DateTime for Last_Update
        df['Last_Updat']=pd.to_datetime(df.Last_Update, infer_datetime_format=True)
        df['Day']=pd.to_datetime(df.Day, infer_datetime_format=True)

        df['Case_Fatality_Ratio']=df['Case_Fatality_Ratio'].astype(float)
        return save_dataframe_to_temp(df)

    def scale_dataframe(**kwargs):
        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='prepare_dataframes')
        print(f'load dataset from {csv_path}')
        df = pd.read_csv(csv_path)
        columns = df.columns
        for c in columns:
            print(c)
        df['Last_Update']=pd.to_datetime(df['Last_Update'], infer_datetime_format=True)
        df['Day']=pd.to_datetime(df['Day'], infer_datetime_format=True)
        df['Case_Fatality_Ratio']=df['Case_Fatality_Ratio'].astype(float)

        Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
        train=df[Selec_Columns]

        min_max_scaler = MinMaxScaler()

        scaled_df = pd.DataFrame(min_max_scaler.fit_transform(train[Selec_Columns]),columns=Selec_Columns)
        scaled_df.index=df.index
        scaled_df['Day']=df['Day']
        print(scaled_df.head(3))
        return save_dataframe_to_temp(scaled_df)


    def clean_up(**kwargs):
         ti = kwargs['ti']
         csv_path = ti.xcom_pull(task_ids='prepare_dataframes')
         print(f'cleanup {csv_path}')
         os.remove(csv_path)
         csv_path = ti.xcom_pull(task_ids='scale_dataframe')
         print(f'cleanup {csv_path}')
         os.remove(csv_path)
         print('cleanup completed')

    def store_to_db(**kwargs):
        host="postgres" # use "localhost" if you access from outside the localnet docker-compose env
        database="airflow"
        user="airflow"
        password="airflow"
        port='5432'
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='scale_dataframe')
        print(f'read csv {csv_path} to database')
        scaled_df = pd.read_csv(csv_path)
        scaled_df.to_sql('scoring_report', engine,if_exists='replace',index=False)
        print('csv file imported')

    def save_scaled_to_csv(**kwargs):
        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='scale_dataframe')
        name = datetime.now().strftime("%y%m%d%H%M%S")
        target = f'/opt/airflow/output/uk_scoring_report_{name}.csv'

        shutil.copyfile(csv_path, target)

    def generate_plot(**kwargs):
        Selec_Columns=['Confirmed','Deaths', 'Recovered', 'Active', 'Incident_Rate','Case_Fatality_Ratio']
        ti = kwargs['ti']
        csv_path = ti.xcom_pull(task_ids='scale_dataframe')
        print(f'read csv {csv_path} to database')
        scaled_df = pd.read_csv(csv_path)
        print('csv file imported')
        scaled_df[Selec_Columns].plot(figsize=(20,10))
        img_name = datetime.now().strftime("%y%m%d%H%M%S")
        plt.savefig(f'/opt/airflow/output/uk_scoring_report_{img_name}.png')

    t1 = PythonOperator(
        task_id='prepare_days_to_fetch',
        python_callable=prepare_days_to_fetch)

    t3 = PythonOperator(
        task_id='prepare_dataframes',
        python_callable=prepare_dataframes)

    t4 = PythonOperator(
        task_id="scale_dataframe",
        python_callable=scale_dataframe
    )

    t5 = PythonOperator(
        task_id = "store_to_db",
        python_callable = store_to_db
    )

    t6 = PythonOperator(
        task_id="clean_up",
        python_callable=clean_up
    )

    t7 = PythonOperator(
        task_id = "generate_plot",
        python_callable=generate_plot
    )

    t8 = PythonOperator(
        task_id = "save_scaled_to_csv",
        python_callable = save_scaled_to_csv
    )


t1>>t3>>t4>>[t5,t8,t7]>>t6
