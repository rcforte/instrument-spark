import pandas as pd
import csv
import json
import jsonpickle
import os.path

def download_sp500():
    print('downloading sp500 data...')
    table = pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    df.to_csv('sp500.csv')

class Instrument(object):
    def __init__(self, symbol, security, gics, gics_sub_industry):
        self.symbol = symbol
        self.security = security
        self.gics = gics
        self.gics_sub_industry = gics_sub_industry
    
    def __repr__(self):
        return f'{self.symbol}, {self.security}, {self.gics}, {self.gics_sub_industry}'

if __name__ == '__main__':
    if not os.path.exists('sp500.csv'):
        download_sp500()
    instruments = []
    if not os.path.exists('sp500.json'):
        print('generating sp500 json file...')
        with open('sp500.csv') as f:
            reader = csv.reader(f)
            next(reader)
            for row in reader:
                instruments.append(Instrument(row[1], row[2], row[4], row[5]))
        with open('sp500.json', 'w+') as f:
            for instrument in instruments:
                instrument_json = jsonpickle.encode(instrument, unpicklable=False)
                f.write(instrument_json + '\n')

    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .appName('SP500_Loader')\
            .config('spark.driver.extraClassPath', './mysql.jar')\
            .getOrCreate()
    df = spark.read.json('sp500.json')
    df.write \
        .format("jdbc")\
        .option("url", "jdbc:mysql://localhost/db_instrument")\
        .option("driver", "com.mysql.jdbc.Driver")\
        .option("dbtable", "tbl_instrument")\
        .option("user", "instrument")\
        .option("password", "instrument")\
        .mode("append")\
        .save()
