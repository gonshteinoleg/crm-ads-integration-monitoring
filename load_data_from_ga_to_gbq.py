""" Скрипт предназначен для выгрузки данных из Google Analytics и загрузки их в BigQuery.
Таблица имеет следующие поля:
- Название сайта
- Дата
- Customer ID
- Campaign ID
- Ad Group ID
- Creative ID
- Тип сделки (eventLabel)
- Номер сделки (eventAction)
- Количество
"""

import pandas as pd
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from GetDataFromGoogleAnalyticsMultipleViews import *
from LoadDataToBigQuery import *

scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive'
)
credentials = Credentials.from_service_account_file('bigquery_secret.json')
credentials = credentials.with_scopes(scopes)
client = bigquery.Client(credentials=credentials)

# Получение списка представлений из BigQuery
query = '''
SELECT 
  viewId
FROM `project.dataset.views`
'''

views = client.query(query, project='project').to_dataframe()

views_list = list(views['viewId'])

yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

# Вызов метода get_data_from_ga_in_loop класса GetDataFromGoogleAnalyticsMultipleViews, который возвращает датафрейм.
get_data_multiple = GetDataFromGoogleAnalyticsMultipleViews (
                                view = views_list,
                                start = yesterday,
                                end = yesterday,
                                dimensions = ['ga:date','ga:hostname','ga:eventAction','ga:eventLabel',
                                              'ga:adwordsCustomerID','ga:adwordsCampaignID',
                                              'ga:adwordsAdGroupID','ga:adwordsCreativeID'],
                                metrics = ['ga:totalEvents'],
                                filter = 'ga:eventCategory==Bitrix_conversions;ga:sourceMedium==google / cpc'
                                )

data_from_ga = get_data_multiple.get_data_from_ga_in_loop()

data_from_ga['date'] = pd.to_datetime(data_from_ga['date']).dt.strftime('%Y-%m-%d')

# Загрузка данных в BigQuery за вчерашний день
load_data_to_bigquery = LoadDataToBigQuery('project',
                                           'dataset', 'google_analytics', data_from_ga)
load_data_to_bigquery.append_data()
