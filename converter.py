import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from db_initial_connection import engine


class ConvertUnits:
    def __init__(self):
        self.units_dict = {0: {'en_name': 'm3', 'name': 'м3'},
                           1: {'en_name': 'kWh', 'name': 'кВтч'},
                           2: {'en_name': 'kcal', 'name': 'ккал'},
                           3: {'en_name': 'kJ', 'name': 'кДж'},
                           4: {'en_name': 'GJ', 'name': 'ГДж'},
                           5: {'en_name': 'therm', 'name': 'терм'},
                           6: {'en_name': 'mbtu', 'name': 'мБТЕ'},
                           7: {'en_name': 'toe', 'name': 'тнэ'},
                           8: {'en_name': 'ft3', 'name': 'куб фут'},
                           9: {'en_name': 'ton of LNG', 'name': 'тонна СПГ'},
                           10: {'en_name': 'm3 of LNG', 'name': 'm3 СПГ'},
                           11: {'en_name': 'MWh', 'name': 'МВтч'},
                           12: {'en_name': 'GWh', 'name': 'ГВтч'},
                           13: {'en_name': 'TWh', 'name': 'ТВтч'},
                           14: {'en_name': 'MJ', 'name': 'МДж'},
                           15: {'en_name': 'mln m3', 'name': 'млн м3'},
                           16: {'en_name': 'th m3', 'name': 'тыс м3'},
                           }

    def calculate_unit_matrix(self, calorific_value):
        cf = np.eye(len(self.units_dict))
        cf[0, 1] = calorific_value
        cf[1, 0] = 1 / calorific_value
        cf[0, 8] = 35.31495452
        cf[8, 0] = 1 / cf[0, 8]
        cf[0, 9] = 0.000724748
        cf[9, 0] = 1 / cf[0, 9]
        cf[0, 10] = 0.001577287
        cf[10, 0] = 1 / cf[0, 10]
        cf[0, 15] = 0.000001
        cf[15, 0] = 1 / cf[0, 15]
        cf[1, 2] = 859.8452279
        cf[2, 1] = 1 / cf[1, 2]
        cf[1, 3] = 3600
        cf[3, 1] = 1 / cf[1, 3]
        cf[1, 4] = 0.0036
        cf[4, 1] = 1 / cf[1, 4]
        cf[1, 6] = 0.00341214163327839
        cf[6, 1] = 1 / cf[1, 6]
        cf[1, 7] = 0.0000859845227858985
        cf[7, 1] = 1 / cf[1, 7]
        cf[1, 11] = 0.001
        cf[11, 1] = 1 / cf[1, 11]
        cf[1, 12] = 0.000001
        cf[12, 1] = 1 / cf[1, 12]
        cf[1, 13] = 0.000000001
        cf[13, 1] = 1 / cf[1, 13]
        cf[1, 14] = 3.6
        cf[14, 1] = 1 / cf[1, 14]
        cf[2, 3] = 4.1868
        cf[3, 2] = 1 / cf[2, 3]
        cf[2, 4] = 0.0000041868
        cf[4, 2] = 1 / cf[2, 4]
        cf[2, 5] = 0.0000396832071950277
        cf[5, 2] = 1 / cf[2, 5]
        cf[2, 6] = 3.96832071950277 * 1.e-6
        cf[6, 2] = 1 / cf[2, 6]
        cf[2, 14] = 0.0041868
        cf[14, 2] = 1 / cf[2, 14]
        cf[3, 4] = 0.000001
        cf[4, 3] = 1 / cf[3, 4]
        cf[3, 6] = 9.47817120355109 * 1.e-7
        cf[6, 3] = 1 / cf[3, 6]
        cf[3, 14] = 0.001
        cf[14, 3] = 1 / cf[3, 14]
        cf[4, 14] = 1000
        cf[14, 4] = 1 / cf[4, 14]
        cf[5, 3] = 105505.5853
        cf[3, 5] = 1 / cf[5, 3]
        cf[5, 7] = 0.002520206
        cf[7, 5] = 1 / cf[5, 7]
        cf[5, 14] = 105.5055853
        cf[14, 5] = 1 / cf[5, 14]
        cf[6, 5] = 10
        cf[5, 6] = 1 / cf[6, 5]
        cf[6, 14] = 1055.055853
        cf[14, 6] = 1 / cf[6, 14]
        cf[7, 3] = 4186800
        cf[3, 7] = 1 / cf[7, 3]
        cf[7, 4] = 41.868
        cf[4, 7] = 1 / cf[7, 4]
        cf[7, 6] = 39.6833521
        cf[6, 7] = 1 / cf[7, 6]
        cf[7, 11] = 11.63
        cf[11, 7] = 1 / cf[7, 11]
        cf[7, 12] = 0.01163
        cf[12, 7] = 1 / cf[7, 12]
        cf[7, 13] = 0.00001163
        cf[13, 7] = 1 / cf[7, 13]
        cf[7, 14] = 41868
        cf[14, 7] = 1 / cf[7, 14]
        cf[11, 12] = 0.001
        cf[12, 11] = 1 / cf[11, 12]
        cf[11, 13] = 0.000001
        cf[13, 11] = 1 / cf[11, 13]
        cf[12, 13] = 0.001
        cf[13, 12] = 1 / cf[12, 13]
        cf[0, 2] = cf[0, 1] * cf[1, 2]
        cf[2, 0] = 1 / cf[0, 2]
        cf[0, 3] = cf[0, 1] * cf[1, 3]
        cf[3, 0] = 1 / cf[0, 3]
        cf[0, 4] = cf[0, 3] * 1.e-6
        cf[4, 0] = 1 / cf[0, 4]
        cf[0, 6] = calorific_value * cf[1, 6]
        cf[6, 0] = 1 / cf[0, 6]
        cf[0, 7] = calorific_value * cf[1, 7]
        cf[7, 0] = 1 / cf[0, 7]
        cf[0, 5] = cf[0, 6] * 10
        cf[5, 0] = 1 / cf[0, 5]
        cf[0, 11] = calorific_value / 1.e3
        cf[11, 0] = 1 / cf[0, 11]
        cf[0, 12] = calorific_value / 1.e6
        cf[12, 0] = 1 / cf[0, 12]
        cf[0, 13] = calorific_value / 1.e9
        cf[13, 0] = 1 / cf[0, 13]
        cf[0, 14] = cf[0, 3] * 1.e-3
        cf[14, 0] = 1 / cf[0, 14]
        cf[1, 8] = cf[1, 0] * cf[0, 8]
        cf[8, 1] = 1 / cf[1, 8]
        cf[9, 1] = cf[9, 0] * calorific_value
        cf[1, 9] = 1 / cf[9, 1]
        cf[1, 5] = cf[1, 6] * 10
        cf[5, 1] = 1 / cf[1, 5]
        cf[1, 10] = cf[1, 0] / 634
        cf[10, 1] = 1 / cf[1, 10]
        cf[1, 15] = cf[1, 0] * 1.e-6
        cf[15, 1] = 1 / cf[1, 15]
        cf[7, 2] = cf[7, 1] / cf[2, 1]
        cf[2, 7] = 1 / cf[7, 2]
        cf[8, 2] = cf[0, 2] * cf[8, 0]
        cf[2, 8] = 1 / cf[8, 2]
        cf[9, 2] = cf[0, 2] * cf[9, 0]
        cf[2, 9] = 1 / cf[9, 2]
        cf[10, 2] = cf[0, 2] * 634
        cf[2, 10] = 1 / cf[10, 2]
        cf[11, 2] = cf[1, 2] * 1000
        cf[2, 11] = 1 / cf[11, 2]
        cf[12, 2] = cf[11, 2] * 1000
        cf[2, 12] = 1 / cf[12, 2]
        cf[13, 2] = cf[12, 2] * 1000
        cf[2, 13] = 1 / cf[13, 2]
        cf[15, 2] = cf[0, 2] * 1.e6
        cf[2, 15] = 1 / cf[15, 2]
        cf[8, 3] = cf[0, 3] * cf[8, 0]
        cf[3, 8] = 1 / cf[8, 3]
        cf[9, 3] = cf[0, 3] * cf[9, 0]
        cf[3, 9] = 1 / cf[9, 3]
        cf[10, 3] = cf[0, 3] * 634
        cf[3, 10] = 1 / cf[10, 3]
        cf[11, 3] = cf[1, 3] * 1000
        cf[3, 11] = 1 / cf[11, 3]
        cf[12, 3] = cf[11, 3] * 1000
        cf[3, 12] = 1 / cf[12, 3]
        cf[13, 3] = cf[12, 3] * 1000
        cf[3, 13] = 1 / cf[13, 3]
        cf[15, 3] = cf[0, 3] * 1.e6
        cf[3, 15] = 1 / cf[15, 3]
        cf[5, 4] = cf[5, 3] * 1.e-6
        cf[4, 5] = 1 / cf[5, 4]
        cf[4, 8] = cf[4, 0] * cf[0, 8]
        cf[8, 4] = 1 / cf[4, 8]
        cf[9, 4] = cf[0, 4] * cf[9, 0]
        cf[4, 9] = 1 / cf[9, 4]
        cf[10, 4] = cf[0, 4] * 634
        cf[4, 10] = 1 / cf[10, 4]
        cf[11, 4] = cf[1, 4] * 1000
        cf[4, 11] = 1 / cf[11, 4]
        cf[12, 4] = cf[11, 4] * 1000
        cf[4, 12] = 1 / cf[12, 4]
        cf[13, 4] = cf[12, 4] * 1000
        cf[4, 13] = 1 / cf[13, 4]
        cf[15, 4] = cf[0, 4] * 1.e6
        cf[4, 15] = 1 / cf[15, 4]
        cf[5, 8] = cf[9, 0] * cf[0, 8]
        cf[8, 5] = 1 / cf[5, 8]
        cf[6, 8] = cf[10, 0] * cf[0, 8]
        cf[8, 6] = 1 / cf[6, 8]
        cf[7, 8] = cf[11, 0] * cf[0, 8]
        cf[8, 7] = 1 / cf[7, 8]
        cf[6, 4] = cf[5, 4] * cf[6, 5]
        cf[4, 6] = 1 / cf[6, 4]
        cf[9, 5] = cf[0, 5] * cf[13, 0]
        cf[5, 9] = 1 / cf[9, 5]
        cf[11, 5] = cf[1, 5] * 1000
        cf[5, 11] = 1 / cf[11, 5]
        cf[10, 5] = cf[0, 5] * 634
        cf[5, 10] = 1 / cf[10, 5]
        cf[9, 6] = cf[0, 6] * cf[9, 0]
        cf[6, 9] = 1 / cf[9, 6]
        cf[6, 10] = cf[6, 0] / 634
        cf[10, 6] = 1 / cf[6, 10]
        cf[5, 12] = cf[5, 11] / 1000
        cf[12, 5] = 1 / cf[5, 12]
        cf[5, 13] = cf[12, 5] / 1000
        cf[13, 5] = 1 / cf[5, 13]
        cf[5, 15] = cf[5, 0] * 1.e-6
        cf[15, 5] = 1 / cf[5, 15]
        cf[6, 11] = cf[6, 1] / 1000
        cf[11, 6] = 1 / cf[6, 11]
        cf[6, 12] = cf[6, 11] / 1000
        cf[12, 6] = 1 / cf[6, 12]
        cf[6, 13] = cf[6, 12] / 1000
        cf[13, 6] = 1 / cf[6, 13]
        cf[6, 15] = cf[6, 0] * 1.e-6
        cf[15, 6] = 1 / cf[6, 15]
        cf[9, 7] = cf[0, 7] * cf[9, 0]
        cf[7, 9] = 1 / cf[9, 7]
        cf[10, 7] = cf[0, 7] * 634
        cf[7, 10] = 1 / cf[10, 7]
        cf[7, 15] = cf[7, 0] * 1.e-6
        cf[15, 7] = 1 / cf[7, 15]
        cf[9, 8] = cf[0, 8] * cf[9, 0]
        cf[8, 9] = 1 / cf[9, 8]
        cf[8, 10] = cf[8, 0] / 634
        cf[10, 8] = 1 / cf[8, 10]
        cf[8, 11] = cf[8, 1] / 1000
        cf[11, 8] = 1 / cf[8, 11]
        cf[8, 12] = cf[8, 11] / 1000
        cf[12, 8] = 1 / cf[8, 12]
        cf[8, 13] = cf[8, 12] / 1000
        cf[13, 8] = 1 / cf[8, 13]
        cf[8, 14] = cf[8, 3] * 1.e-3
        cf[14, 8] = 1 / cf[8, 14]
        cf[8, 15] = cf[8, 0] * 1.e-6
        cf[15, 8] = 1 / cf[8, 15]
        cf[9, 10] = cf[9, 0] / 634
        cf[10, 9] = 1 / cf[9, 10]
        cf[9, 11] = cf[9, 1] / 1000
        cf[11, 9] = 1 / cf[9, 11]
        cf[9, 12] = cf[9, 11] / 1000
        cf[12, 9] = 1 / cf[9, 12]
        cf[9, 13] = cf[9, 12] / 1000
        cf[13, 9] = 1 / cf[9, 13]
        cf[9, 14] = cf[9, 3] * 1.e-3
        cf[14, 9] = 1 / cf[9, 14]
        cf[9, 15] = cf[9, 0] * 1.e-6
        cf[15, 9] = 1 / cf[9, 15]
        cf[10, 11] = cf[10, 1] / 1000
        cf[11, 10] = 1 / cf[10, 11]
        cf[10, 12] = cf[10, 11] / 1000
        cf[12, 10] = 1 / cf[10, 12]
        cf[10, 13] = cf[10, 12] / 1000
        cf[13, 10] = 1 / cf[10, 13]
        cf[10, 14] = cf[10, 4] * 1.e-3
        cf[14, 10] = 1 / cf[10, 14]
        cf[10, 15] = cf[10, 0] * 1.e-6
        cf[15, 10] = 1 / cf[10, 15]
        cf[11, 14] = cf[11, 3] * 1.e-3
        cf[14, 11] = 1 / cf[11, 14]
        cf[11, 15] = cf[11, 0] * 1.e-6
        cf[15, 11] = 1 / cf[11, 15]
        cf[12, 14] = cf[12, 3] * 1.e-3
        cf[14, 12] = 1 / cf[12, 14]
        cf[12, 15] = cf[12, 0] * 1.e-6
        cf[15, 12] = 1 / cf[12, 15]
        cf[13, 14] = cf[13, 0] * 1.e-3
        cf[14, 13] = 1 / cf[13, 14]
        cf[13, 15] = cf[13, 0] * 1.e-6
        cf[15, 13] = 1 / cf[13, 15]
        cf[14, 15] = cf[14, 0] * 1.e-6
        cf[15, 14] = 1 / cf[14, 15]
        cf[16, 0] = 1000
        cf[0, 16] = 1 / cf[16, 0]
        cf[16, 1] = cf[0, 1] * 1000
        cf[1, 16] = 1 / cf[16, 1]
        cf[16, 2] = cf[0, 2] * 1000
        cf[2, 16] = 1 / cf[16, 2]
        cf[16, 3] = cf[0, 3] * 1000
        cf[3, 16] = 1 / cf[16, 3]
        cf[16, 4] = cf[0, 4] * 1000
        cf[4, 16] = 1 / cf[16, 4]
        cf[16, 5] = cf[0, 5] * 1000
        cf[5, 16] = 1 / cf[16, 5]
        cf[16, 6] = cf[0, 6] * 1000
        cf[6, 16] = 1 / cf[16, 6]
        cf[16, 7] = cf[0, 7] * 1000
        cf[7, 16] = 1 / cf[16, 7]
        cf[16, 8] = cf[0, 8] * 1000
        cf[8, 16] = 1 / cf[16, 8]
        cf[16, 9] = cf[0, 9] * 1000
        cf[9, 16] = 1 / cf[16, 9]
        cf[16, 10] = cf[0, 10] * 1000
        cf[10, 16] = 1 / cf[16, 10]
        cf[16, 11] = cf[0, 11] * 1000
        cf[11, 16] = 1 / cf[16, 11]
        cf[16, 12] = cf[0, 12] * 1000
        cf[12, 16] = 1 / cf[16, 12]
        cf[16, 13] = cf[0, 13] * 1000
        cf[13, 16] = 1 / cf[16, 13]
        cf[16, 14] = cf[0, 14] * 1000
        cf[14, 16] = 1 / cf[16, 14]
        cf[16, 15] = cf[0, 15] * 1000
        cf[15, 16] = 1 / cf[16, 15]
        return cf

    def calculate(self, calorific_value, input_unit, output_unit, input_value):
        df = pd.DataFrame(self.units_dict)
        from_unit = df.apply(lambda row: row[row == input_unit], axis=1).columns.values[0]
        to_unit = df.apply(lambda row: row[row == output_unit], axis=1).columns.values[0]
        cf = self.calculate_unit_matrix(calorific_value)
        multiplier = cf[from_unit, to_unit]
        output_value = format(round(input_value * multiplier, 5), '.5f')
        return output_value, multiplier


class ConvertCurrencies:
    @staticmethod
    def calculate(input_value, input_currency, output_currency, date):
        target_date = datetime.strptime(date, '%Y-%m-%d')
        last_date = datetime.strptime(date, '%Y-%m-%d') - timedelta(days=7)
        df = pd.read_sql(
            f"SELECT * FROM f_get_currency_values('{input_currency}',"
            f" '{output_currency}', '{last_date}', '{target_date}')",
            con=engine)
        if df.empty:
            df = pd.read_sql(
                f"SELECT * FROM f_get_currency_values('{output_currency}',"
                f" '{input_currency}', '{last_date}', '{target_date}')",
                con=engine)
            numpy_date = df[df['date'] == df['date'].max()]['date'].to_numpy()[0]
            ts = pd.to_datetime(str(numpy_date))
            date = ts.strftime('%Y-%m-%d')
            multiplier = 1 / df[df['date'] == df['date'].max()]['value'].to_numpy()[0]
            output_value = format(round(input_value * multiplier, 5), '.5f')
            return output_value, str(target_date.date()), date, multiplier
        else:
            numpy_date = df[df['date'] == df['date'].max()]['date'].to_numpy()[0]
            ts = pd.to_datetime(str(numpy_date))
            date = ts.strftime('%Y-%m-%d')
            multiplier = df[df['date'] == df['date'].max()]['value'].to_numpy()[0]
            output_value = format(round(input_value * multiplier, 5), '.5f')
            return output_value, str(target_date.date()), date, multiplier


class ConvertPrices:
    @staticmethod
    def calculate(input_value, input_currency, output_currency, date, calorific_value, input_unit,
                  output_unit):
        currencies_multiplier = ConvertCurrencies.calculate(
            input_value=1,
            input_currency=input_currency,
            output_currency=output_currency,
            date=date)[0]

        units_multiplier = ConvertUnits().calculate(
            input_value=1,
            calorific_value=calorific_value,
            input_unit=input_unit,
            output_unit=output_unit,
        )[0]

        output_value = input_value * float(currencies_multiplier) / float(units_multiplier)
        return output_value
