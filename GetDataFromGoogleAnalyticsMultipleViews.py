import pandas as pd
import time
from gaapi4py import GAClient
from GetDataFromGoogleAnalytics import *

SERVICE_KEY = 'ga-api-secret.json'
c = GAClient(SERVICE_KEY)


class GetDataFromGoogleAnalyticsMultipleViews(GetDataFromGoogleAnalytics):
    """ Класс для получения данных сразу из нескольких представлений Google Analytics.
    Получает на вход свойства родительского класса, но вместо строки, в параметр view передается список представлений.
    """

    def get_data_from_ga_in_loop(self):
        report_df = pd.DataFrame()

        for v in self.view:
            try:
                self.view = v
                response = super().get_data_from_ga()
                report_df = report_df.append(response, ignore_index=True)
            except Exception as e:
                print(e, v)
            time.sleep(5)
        return report_df
