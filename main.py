import csv
from datetime import datetime
import time
import pandas as pd
import math
import os.path

importPath = "raw/"
exportPath = "rates/"

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

pairs = ['EURUSD', 'EURCHF', 'EURGBP', 'EURJPY',
         'EURAUD', 'USDCAD', 'USDCHF', 'USDJPY',
         'USDMXN', 'GBPCHF', 'GBPJPY', 'GBPUSD',
         'AUSJPY', 'AUDUSD', 'CHFJPY', 'NZDJPY',
         'NZDUSD', 'XAUUSD']


def loadFiles(pair):
    rows = []
    inputFiles = [
        "DAT_ASCII_" + pair + "_M1_202201.csv",
        "DAT_ASCII_" + pair + "_M1_202202.csv",
        "DAT_ASCII_" + pair + "_M1_202203.csv",
        "DAT_ASCII_" + pair + "_M1_202204.csv",
    ]
    for file in inputFiles:
        rows = loadCsv(importPath + file, rows)

    print(f'  found {len(rows)} lines.')

    return rows


def loadCsv(file, rows):
    if os.path.exists(file):
        with open(file) as f:
            # DateTime, Open, High, Low, Close, Volume
            csv_reader = csv.reader(f, delimiter=';')

            for row in csv_reader:
                dateTime = datetime.strptime(row[0], '%Y%m%d %H%M%S')

                rows.append([int(dateTime.timestamp()),
                            row[1], row[2], row[3], row[4]])

    return rows


def saveCsv(file, rows):
    with open(exportPath + file, 'w', encoding='UTF8', newline='') as f:
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

for pair in pairs:
    print(f'Processing {pair} trade history')
    rates_m1 = loadFiles(pair)

    if len(rates_m1) > 0:
        for frame, interval in frames.items():
            print(f'  exporting {frame} rates')
            rates = getRatesForInterval(interval, rates_m1)
            filename = pair + '_' + frame + '_2022.csv'
            saveCsv(filename, rates)

exec_time = time.time() - start_time
print("Execution time: %s seconds" % exec_time)
