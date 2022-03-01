""" Скрипт предназначен для формирования и загрузки в BigQuery таблицы со следующими полями:
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
from LoadDataToBigQuery import *

scopes = (
        'https://www.googleapis.com/auth/bigquery',
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/drive'
)
credentials = Credentials.from_service_account_file('bigquery_secret.json')
credentials = credentials.with_scopes(scopes)
client = bigquery.Client(credentials=credentials)

# Получаем из GBQ данные из CRM
query = '''
SELECT 
  deal_id, 
  date, 
  lead_date, 
  source, 
  utm_source, 
  google_client_id, 
  click_id, 
  url, 
  previous_stage, 
  current_stage, 
  conversion_sent, 
  property_id,
  NET.HOST(url) as hostname
FROM `project.dataset.crm`
WHERE utm_source = "adwords"
'''
crm = client.query(query, project='project').to_dataframe()

# Получаем из BigQuery данные из Google Ads
query = '''
SELECT *
FROM `project.dataset.google_ads`
WHERE date = DATE_SUB(current_date, INTERVAL 1 DAY)
'''
google_ads = client.query(query, project='project').to_dataframe()

# Получаем из BigQuery данные из Google Analytics
query = '''
SELECT *
FROM `project.dataset.google_analytics`
'''
google_analytics = client.query(query, project='project').to_dataframe()

# Добавляем в данные из Google Ads и Google Analytics ключ, состоящий из Customer ID, Campaign ID и Ad Group Id
google_ads['key'] = google_ads['external_customer_id'] + google_ads['campaign_id'] + google_ads['ad_group_id']
google_analytics['key'] = (google_analytics['adwordsCustomerID'] + 
google_analytics['adwordsCampaignID'] + google_analytics['adwordsAdGroupID'])

# Объединяем данные Google Ads и Google Analytics
google_ads = google_ads.merge(google_analytics[['key', 'hostname']], how='left', on='key')
google_ads = google_ads.fillna('(not set)')

# Считаем количество сделок для каждого дня и сайта в данных CRM
crm_deals = (crm.groupby(['hostname', 'date'])
             .agg({'deal_id':'count'})
             .reset_index()
             .sort_values(by='deal_id',ascending=False)
             .rename(columns={'deal_id':'crm_deals'}))

# Считаем количество сделок для каждого дня и сайта в данных Google Analytics
google_analytics['totalEvents'] = google_analytics['totalEvents'].astype(int)
ga_deals = (google_analytics.groupby(['hostname', 'date'])
            .agg({'totalEvents':'sum'})
            .reset_index()
            .sort_values(by='totalEvents',ascending=False)
            .rename(columns={'totalEvents':'ga_deals'}))

# Считаем количество сделок для каждого дня и сайта в данных CRM
google_ads_deals = (google_ads.groupby(['hostname', 'date'])
           .agg({'conversions':'sum'})
           .reset_index()
           .sort_values(by='conversions',ascending=False)
           .rename(columns={'conversions':'ads_deals'}))

# Приводим во всех 3 датафреймах дату в один формат
ga_deals['date'] = pd.to_datetime(ga_deals['date'])
google_ads_deals['date'] = pd.to_datetime(google_ads_deals['date'])
crm_deals['date'] = pd.to_datetime(crm_deals['date'])

# Формируем один датафрейм
result_df = (crm_deals
             .merge(ga_deals[['hostname', 'date', 'ga_deals']], how='left', on=['hostname', 'date'])
             .merge(google_ads_deals[['hostname', 'date', 'ads_deals']], how='left', on=['hostname', 'date']))

# Считаем расхождения между системами
result_df = result_df.fillna(0)
result_df['ga_vs_crm'] = round(100 - result_df['ga_deals'] / result_df['crm_deals'] * 100, 1)
result_df['ads_vs_crm'] = round(100 - result_df['ads_deals'] / result_df['crm_deals'] * 100, 1)
result_df['ga_vs_crm'] = result_df['ga_vs_crm'].astype(str) + '%'
result_df['ads_vs_crm'] = result_df['ads_vs_crm'].astype(str) + '%'

# Считаем для каждого дня и сайта количество сделок без gclid
crm_without_gclid = crm.fillna(0).query('click_id == 0')
crm_without_gclid = (crm_without_gclid.groupby(['hostname', 'date'])
                     .agg({'deal_id':'count'})
                     .reset_index()
                     .sort_values(by='deal_id',ascending=False)
                     .rename(columns={'deal_id':'crm_deals'}))

crm_without_gclid.columns = [['hostname', 'date', 'deals_without_gclid']]
crm_without_gclid_clip['date'] = pd.to_datetime(crm_without_gclid_clip['date'])

# Костыль! Достаем датафрейм из буфера обмена, иначе по неизвестной причине не получилось смержить его с result_df
crm_without_gclid.to_clipboard(index=False)
crm_without_gclid_clip=pd.read_clipboard(sep='\t')

# Объединяем основной датафрейм с данными о сделках без gclid
result_df = pd.merge(result_df, crm_without_gclid_clip, on=['hostname', 'date'], how='left')

# Удаляем таблицу, чтобы исключить кейс, когда данные не перезаписываются, а добавляются
try:
    dataset_ref = client.dataset('dataset')
    dataset = bigquery.Dataset(dataset_ref)
    table_ref = dataset_ref.table('statistics')
    client.delete_table(table_ref)
except:
    pass

# Загружаем данные в BigQuery
load_data_to_bigquery = LoadDataToBigQuery('project',
                                           'dataset', 'statistics', result_df)

load_data_to_bigquery.truncate_data()
