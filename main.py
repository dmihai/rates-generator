import csv
from datetime import datetime
import time
import pandas as pd
import math
from os import listdir
from os.path import isfile, join, exists

inputPath = "raw/"
outputPath = "rates/"

years = ["2022", "2021", "2020", "2019"]

pairs = ['EURUSD', 'EURCHF', 'EURGBP', 'EURJPY',
         'EURAUD', 'USDCAD', 'USDCHF', 'USDJPY',
         'USDMXN', 'GBPCHF', 'GBPJPY', 'GBPUSD',
         'AUSJPY', 'AUDUSD', 'CHFJPY', 'NZDJPY',
         'NZDUSD', 'XAUUSD']

frames = {
    "M1":             1 * 60,
    "M3":             3 * 60,
    "M5":             5 * 60,
    "M10":           10 * 60,
    "M15":           15 * 60,
    "M30":           30 * 60,
    "H1":        1 * 60 * 60,
    "H2":        2 * 60 * 60,
    "H3":        3 * 60 * 60,
    "H4":        4 * 60 * 60,
    "H6":        6 * 60 * 60,
    "D1":   1 * 24 * 60 * 60,
    "W1":   7 * 24 * 60 * 60,
    "MN1": 30 * 24 * 60 * 60,
}


def loadFiles(pair, year):
    rows = []
    inputFiles = (f for f in files if '_'+pair+'_' in f and '_'+year in f)

    for file in inputFiles:
        rows = loadCsv(inputPath + file, rows)

    print(f'  found {len(rows)} lines.')

    return rows


def loadCsv(file, rows):
    if exists(file):
        with open(file) as f:
            # DateTime, Open, High, Low, Close, Volume
            csv_reader = csv.reader(f, delimiter=';')

            for row in csv_reader:
                dateTime = datetime.strptime(row[0], '%Y%m%d %H%M%S')

                rows.append([int(dateTime.timestamp()),
                            row[1], row[2], row[3], row[4]])

    return rows


def saveCsv(file, rows):
    with open(outputPath + file, 'w', encoding='UTF8', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(rows)


def getRatesForInterval(interval, rows):
    intervalRows = []
    currentTime = rows[0][0]
    openPrice = rows[0][1]
    highPrice = float(rows[0][2])
    lowPrice = float(rows[0][3])
    closePrice = rows[0][4]
    for row in rows:
        if currentTime + interval > row[0]:
            if float(row[2]) > highPrice:
                highPrice = float(row[2])
            if float(row[3]) < lowPrice:
                lowPrice = float(row[3])
            closePrice = row[4]
        else:
            intervalRows.append(
                [currentTime, openPrice, highPrice, lowPrice, closePrice])
            currentTime = currentTime + (math.floor(
                (row[0] - currentTime) / interval) * interval)
            # if currentTime != row[0]:
            #     print(currentTime, row[0])
            openPrice = row[1]
            highPrice = float(row[2])
            lowPrice = float(row[3])
            closePrice = row[4]

    intervalRows.append(
        [currentTime, openPrice, highPrice, lowPrice, closePrice])

    return intervalRows


start_time = time.time()
files = [f for f in listdir(inputPath) if isfile(
    join(inputPath, f)) and '.csv' in f]
files.sort()

for pair in pairs:
    for year in years:
        print(f'Processing {pair} trade history for year {year}')
        rates_m1 = loadFiles(pair, year)

        if len(rates_m1) > 0:
            for frame, interval in frames.items():
                print(f'  exporting {frame} rates')
                rates = getRatesForInterval(interval, rates_m1)
                filename = pair + '_' + frame + '_' + year + '.csv'
                saveCsv(filename, rates)

exec_time = time.time() - start_time
print("Execution time: %s seconds" % exec_time)
