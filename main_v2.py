import csv
from datetime import datetime
import time
import math
from os import listdir, mkdir
from os.path import isfile, isdir, join, exists
import argparse
import pandas as pd

inputPath = "raw/"
outputPath = "rates/"

pairs = ['EURUSD', 'EURCHF', 'EURGBP', 'EURJPY',
         'EURAUD', 'USDCAD', 'USDCHF', 'USDJPY',
         'USDMXN', 'GBPCHF', 'GBPJPY', 'GBPUSD',
         'AUDJPY', 'AUDUSD', 'CHFJPY', 'NZDJPY',
         'NZDUSD', 'XAUUSD', 'AUDCAD', 'AUDCHF',
         'AUDNZD', 'AUXAUD', 'BCOUSD', 'CADCHF',
         'CADJPY', 'EURCAD', 'EURCZK', 'EURDKK',
         'EURHUF', 'EURNOK', 'EURNZD', 'EURPLN',
         'EURSEK', 'EURTRY', 'FRXEUR', 'GBPAUD',
         'GBPCAD', 'GBPNZD', 'GRXEUR', 'HKXHKD',
         'JPXJPY', 'NSXUSD', 'NZDCAD', 'NZDCHF',
         'SGDJPY', 'SPXUSD', 'UDXUSD', 'UKXGBP',
         'USDCZK', 'USDDKK', 'USDHKD', 'USDHUF',
         'USDNOK', 'USDPLN', 'USDSEK', 'USDSGD',
         'USDTRY', 'USDZAR', 'WTIUSD', 'XAGUSD',
         'ZARJPY']

frames = {
    "M1": "T",
    "M3": "3T",
    "M5": "5T",
    "M10": "10T",
    "M15": "15T",
    "M30": "30T",
    "H1": "H",
    "H2": "2H",
    "H3": "3H",
    "H4": "4H",
    "H6": "6H",
    "D1": "D",
    "W1": "W-MON",
    "MN1": "MS",
}


def loadFiles(pair, year):
    df = pd.DataFrame()
    inputFiles = (f for f in files if '_'+pair+'_' in f and '_'+year in f)

    for file in inputFiles:
        df = loadCsv(inputPath + file, df)

    print(f'  found {len(df)} lines.')

    return df


def loadCsv(file, df):
    if exists(file):
        data = pd.read_csv(file,
                           sep=';',
                           header=None,
                           names=['timestamp', 'open_price',
                                  'high_price', 'low_price', 'close_price', 'volume'],
                           parse_dates=['timestamp'],
                           date_parser=lambda x: datetime.strptime(
                               x+" -0500", '%Y%m%d %H%M%S %z'))
        data['timestamp'] = data['timestamp'].dt.tz_convert('Etc/GMT-2')
        df = pd.concat([df, data])

    return df


def saveCsv(pair, frame, year, df):
    filename = f"{pair}_{frame}_{year}.csv"
    filepath = f"{outputPath}/{pair}"
    if not isdir(filepath):
        mkdir(filepath)

    df.to_csv(f"{filepath}/{filename}", encoding='utf-8', header=False)


def getRatesForInterval(interval, df):
    return df.resample(interval, on='timestamp').agg({
        'open_price': 'first',
        'high_price': 'max',
        'low_price': 'min',
        'close_price': 'last'}).dropna()


start_time = time.time()

parser = argparse.ArgumentParser()
parser.add_argument('--year', type=str, required=True,
                    help='Select the year (2022)')
parser.add_argument('--pair', type=str, required=False, default='',
                    help='Select the currency pair (EURUSD) or leave blank to select all')
args = parser.parse_args()

if args.pair != '':
    pairs = [args.pair]

files = [f for f in listdir(inputPath) if isfile(
    join(inputPath, f)) and '.csv' in f]
files.sort()

for pair in pairs:
    print(f'Processing {pair} trade history for year {args.year}')
    rates_m1 = loadFiles(pair, args.year)

    if len(rates_m1) > 0:
        for frame, interval in frames.items():
            print(f'  exporting {frame} rates')
            rates = getRatesForInterval(interval, rates_m1)
            saveCsv(pair, frame, args.year, rates)

exec_time = time.time() - start_time
print("Execution time: %s seconds" % exec_time)
