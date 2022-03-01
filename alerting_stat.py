""" Скрипт предназначен для отправки на электронную почту таблицы со сводкой, состоящей из следующих полей:
- Название сайта
- Дата
- Количество сделок в CRM
- Количество сделок в Google Analytics
- Количество сделок в Google Ads
- Разница в % между количеством сделок в Google Analytics и CRM
- Разница в % между количеством сделок в Google Ads и CRM
- Количество сделок без Google Click iD
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

# Получаем данные из BigQuery с условием, что расхождения между CRM и Google Ads более 10%
query = '''
SELECT 
  hostname,
  CAST(date AS DATE) as date,
  crm_deals,
  ga_deals,
  ads_deals,
  ga_vs_crm,
  ads_vs_crm,
  deals_without_gclid
FROM `project.dataset.statistics`
WHERE CAST(REGEXP_EXTRACT(ads_vs_crm, r"([^%]*)") AS FLOAT64) > 10
OR CAST(REGEXP_EXTRACT(ads_vs_crm, r"([^%]*)") AS FLOAT64) < -10
'''

statistics = client.query(query, project='project').to_dataframe()

# Перевод датафрейма в формат HTML-таблицы
html_table = build_table(statistics, 'blue_light', font_size='15.3px',
                            font_family='Open Sans,sans-serif',
                            text_align='right',
                            width='210px',
                            index=False,
                            even_color='black',
                            even_bg_color='white'
                            )

# Отправляем сообщение на почту
email_content = 'Проблемы в интеграции на ' + datetime.now().strftime('%Y-%m-%d') + html_table

email = Email(email_content)
email.send_email()
