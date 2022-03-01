""" Скрипт предназначен для отправки на электронную почту сообщения в случае,
если количество строк в выгрузке из CRM снизилось больше, чем на 30%
"""

import pandas as pd
from datetime import datetime, timedelta
import time
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

from Email import *

import warnings
warnings.filterwarnings('ignore')

scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive'
)
credentials = Credentials.from_service_account_file('bigquery_secret.json')
credentials = credentials.with_scopes(scopes)
client = bigquery.Client(credentials=credentials)

# Получаем данные из BigQuery
query = '''
SELECT
  a.metric,
  yesterday,
  today,
  ROUND(today/yesterday*100, 1) as diff
FROM
(
  SELECT 
    "count_rows" as metric, 
    COUNT(*) as today
  FROM `project.dataset.crm`
  WHERE date = DATE_SUB(current_date, INTERVAL 1 DAY)
) as a
LEFT JOIN
(
  SELECT 
    "count_rows" as metric, 
    COUNT(*) as yesterday
  FROM `project.dataset.crm`
  WHERE date = DATE_SUB(current_date, INTERVAL 2 DAY)
) as b on a.metric=b.metric
'''

crm = client.query(query, project='project').to_dataframe()

difference = crm['diff'][0]

if difference < 70:
    message = f'За вчерашний день в выгрузке из CRM количество строк составляет {difference}% от количества за предыдущий день, проверьте интеграцию.'
else:
    message = ''

# Отправляем сообщение на почту
if len(message) > 0:
    email = Email(message)
    email.send_email()
