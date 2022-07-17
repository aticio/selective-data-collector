import pandas as pd
import requests
import os
import csv
import time
import json
from os import listdir

AV_APIKEY = os.environ["AV_APIKEY"]

slices = ["year1month1","year1month2", "year1month3", "year1month4", "year1month5", "year1month6", "year1month7", "year1month8", "year1month9", "year1month10", "year1month11", "year1month12", "year2month1", "year2month2", "year2month3", "year2month4", "year2month5", "year2month6", "year2month7", "year2month8", "year2month9", "year2month10", "year2month11", "year2month12"]

def main():
    symbols = ["SH", "SQQQ", "SDS"]

    for sy in symbols:
        print(f"Collecting data for {sy}")
        data = []
        counter = 0
        for s in slices:
            partial_data = get_data(sy, s)
            for p in partial_data:
                data.append(p)
            print(len(partial_data), len(data))
            counter = counter + 1
            if counter == 5:
                time.sleep(60)
                counter = 0
        json_string = json.dumps(data)
        write_to_file(sy, json_string)
        print(f"Data collection is finished for {sy}")
        time.sleep(60)


def write_to_file(symbol, json_string):
    with open(f'data/{symbol}.json', 'w') as f:
        f.write(json_string)



def get_data(symbol, slice):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval=1min&slice={slice}&apikey={AV_APIKEY}'
    with requests.Session() as s:
        download = s.get(url)
        decoded_content = download.content.decode('utf-8')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        my_list = list(cr)
        my_list.pop(0)
        return my_list


def get_symbols():
    symbols_table = pd.read_html("https://en.wikipedia.org/wiki/List_of_S%26P_500_companies", header=0)[0]
    symbols = list(symbols_table.loc[:, "Symbol"])
    return symbols


if __name__ == "__main__":
    main()
