import pandas as pd
from gaapi4py import GAClient

SERVICE_KEY = 'ga-api-secret.json'
c = GAClient(SERVICE_KEY)


class GetDataFromGoogleAnalytics:
    """ Класс для получения данных из Google Analytics.
    Получает на вход:
    - Представление
    - Начало отчета
    - Конец отчета
    - Показатель или список показателей
    - Метрику или список метрик
    - Фильтр (по умолчанию не заполняется)
    """

    def __init__(self, view, start, end, dimensions, metrics, filter=None):
        self.view = view
        self.start = start
        self.end = end
        self.dimensions = dimensions
        self.metrics = metrics
        self.filter = filter

    def _transform_input(self, obj):
        if type(obj) == list:
            return set(obj)
        if type(obj) == str:
            return {obj}
        else:
            print("Неожидаемый тип данных")

    def get_data_from_ga(self):
        c.set_view_id(self.view)
        c.set_dateranges(self.start, self.end)
        request_body = {
            'dimensions': self._transform_input(self.dimensions),
            'metrics': self._transform_input(self.metrics),
            'filter': self.filter
        }
        df = c.get_all_data(request_body)
        df = df['data']
        return df
