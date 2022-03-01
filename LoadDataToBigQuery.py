import pandas as pd
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive'
)
credentials = Credentials.from_service_account_file('bigquery_secret.json')
credentials = credentials.with_scopes(scopes)
client = bigquery.Client(credentials=credentials)


class LoadDataToBigQuery:
        """ Класс для загрузки данных в Google BigQuery.
        Получает на вход:
        - Название проекта
        - Название датасета
        - Название таблицы
        - Датафрейм, который требуется загрузить
        """

        def __init__(self, project, dataset, table, df):
                self.project = project
                self.dataset = dataset
                self.table = table
                self.df = df
        
        # Метод для загрузки данных в новую таблицу
        def truncate_data(self):
                dataset_ref = client.dataset(self.dataset)
                dataset = bigquery.Dataset(dataset_ref)

                table_ref = dataset_ref.table(self.table)

                client.load_table_from_dataframe(self.df, table_ref).result()

        # Метод для добавления данных в уже созданную таблицу
        def append_data(self):
                df_dict = self.df.to_dict('r')  # df - датафрейм с данными за 1 день, которые нужно загрузить
                table = client.get_table(f'{self.project}.{self.dataset}.{self.table}')
                rows_to_insert = df_dict
                client.insert_rows(table, rows_to_insert)
