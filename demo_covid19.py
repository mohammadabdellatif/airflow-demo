
from datetime import timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
from airflow.utils.dates import days_ago

import pandas as pd
import tempfile as tf
from sklearn.preprocessing import MinMaxScaler

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
        List_of_days=[]
        for year in range(2021,2022):
          for month in range(1,3):
            for day in range(1,32):
              month=int(month)
              if day <=9:
                day=f'0{day}'

              if month <= 9 :
                month=f'0{month}'
              List_of_days.append(f'{month}-{day}-{year}')
        return List_of_days

    def print_dates(**kwargs):
        ti = kwargs['ti']
        dates = ti.xcom_pull(task_ids='prepare_days_to_fetch')
        for d in dates:
            print(d)

    def get_df_by_date(Day):
        DF_i=None
        try:
            URL_Day=f'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/{Day}.csv'
            DF_day=pd.read_csv(URL_Day)
            DF_day['Day']=Day
            cond=(DF_day.Country_Region=='India')&(DF_day.Province_State=='Delhi')
            Selec_columns=['Day','Country_Region', 'Last_Update',
                  'Lat', 'Long_', 'Confirmed', 'Deaths', 'Recovered', 'Active',
                  'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
            DF_i=DF_day[cond][Selec_columns].reset_index(drop=True)
        except:
            pass
        return DF_i

    def prepare_dataframes(**kwargs):
        print('start to fetch datasets')
        ti = kwargs['ti']
        dates = ti.xcom_pull(task_ids='prepare_days_to_fetch')
        DF_all=[]
        for d in dates:
            df = get_df_by_date(d)
            DF_all.append(df)
        l = len(DF_all)
        print(f'total dataframes retrieved {l}')
        df=pd.concat(DF_all).reset_index(drop=True)
        # Create DateTime for Last_Update
        df['Last_Updat']=pd.to_datetime(df.Last_Update, infer_datetime_format=True)
        df['Day']=pd.to_datetime(df.Day, infer_datetime_format=True)

        df['Case_Fatality_Ratio']=df['Case_Fatality_Ratio'].astype(float)
        td = tf.mkdtemp()
        csv_path = f'{td}/data.csv'
        df.to_csv(csv_path)
        print(f'csv data saved to {csv_path}')
        return csv_path
        
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
        scaled_df.index=scaled_df.index
        scaled_df['Day']=scaled_df.Day
        print(scaled_df.take(3))


    t1 = PythonOperator(
        task_id='prepare_days_to_fetch',
        python_callable=prepare_days_to_fetch)

    t2 = PythonOperator(
        task_id='print_dates',
        python_callable=print_dates)

    t3 = PythonOperator(
        task_id='prepare_dataframes',
        python_callable=prepare_dataframes)

    t4 = PythonOperator(
        task_id="scale_dataframe",
        python_callable=scale_dataframe
    )

t1>>t2>>t3 >> t4
