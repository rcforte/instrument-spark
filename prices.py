import mysql.connector
import yfinance as yf
import pandas as pd
import os.path
import csv
import jsonpickle

class MarketData(object):
    def __init__(self, type, sym, date, open, high, low, 
            close, volume, dividends, stock_splits):
        self.type = str(type)
        self.symbol = str(sym)
        self.date = str(date)
        self.open = float(open) if open else None
        self.high = float(high) if high else None
        self.low = float(low) if low else None
        self.close = float(close) if close else None
        self.volume = float(volume) if volume else None
        self.dividends = float(dividends) if dividends else None
        self.stock_splits = float(stock_splits) if stock_splits else None

def get_connection():
    return mysql.connector.connect(
        user='instrument', 
        password='instrument', 
        host='localhost', 
        database='db_instrument'
    )

def generate_json():
    con = get_connection()
    cur = con.cursor()
    cur.execute('select symbol from tbl_instrument')
    for row in cur:

        # skip if csv exists
        sym = row[0]
        filein = sym + '.csv'
        if not os.path.exists(filein):
            print(f'downloading prices for {sym} ...')
            ts = yf.Ticker(sym)
            prices = ts.history(period='max')
            prices.to_csv(filein)

        # skip if json exists
        fileout = 'prices/' + sym + '.json'
        if os.path.exists(fileout):
            print(f'skipping file {fileout} ...')
            continue

        # generate json from csv
        print(f'generating file {fileout} ...')
        mkts = []
        with open(filein) as f:
            reader = csv.reader(f)
            next(reader) # skip header
            for r in reader:
                try:
                    mkt = MarketData(
                        'PRICE', sym, 
                        r[0], r[1], r[2], r[3], 
                        r[4], r[5], r[6], r[7]
                    )
                    mkts.append(mkt)
                except ValueError as err:
                    print(f'{sym} date: {r}')
        with open(fileout, 'w+') as f:
            for mkt in mkts:
                ss = jsonpickle.encode(mkt, unpicklable=False)
                f.write(ss + '\n')
    cur.close();
    con.close()

if __name__ == '__main__':
    generate_json()

    if False: # don't need this due to truncate
        print('removing market data ...')
        con = get_connection()
        cur = con.cursor()
        cur.execute('delete from tbl_market_data')
        cur.close()
        con.close()

    print('inserting prices ...')
    from pyspark.sql import SparkSession
    spark = SparkSession\
            .builder\
            .appName('SP500_Prices_Loader')\
            .master('local[*]')\
            .config('spark.driver.extraClassPath', './mysql.jar')\
            .getOrCreate()
    prices_df = spark.read.json('prices/*.json')
    prices_df.createTempView('prices')
    prices_df.show()

    instrument_df = spark.read\
        .format("jdbc")\
        .option('url', 'jdbc:mysql://localhost/db_instrument')\
        .option('driver', 'com.mysql.jdbc.Driver')\
        .option('dbtable', 'tbl_instrument')\
        .option('user', 'instrument')\
        .option('password', 'instrument')\
        .load()
    instrument_df.createTempView('instrument')
    instrument_df.show()

    joined = spark.sql('''
        select type, 
               instrument.id as instrument_id, 
               date, 
               open, 
               high, 
               low, 
               close, 
               volume, 
               dividends, 
               stock_splits
        from prices
            inner join instrument
                on prices.symbol = instrument.symbol
    ''')
    joined.show()
    joined.printSchema()
    joined.write \
        .format('jdbc')\
        .option('url', 'jdbc:mysql://localhost/db_instrument')\
        .option('driver', 'com.mysql.jdbc.Driver')\
        .option('dbtable', 'tbl_market_data')\
        .option('user', 'instrument')\
        .option('password', 'instrument')\
        .option('truncate', True)\
        .mode('overwrite')\
        .save()



