import os
from datetime import datetime

from private.futures.data_preparation_utils import generate_multiple_price

multiple_price_dir = 'multiple_prices'
if not os.path.exists(multiple_price_dir):
    os.makedirs(multiple_price_dir)

adjust_price_dir = 'adjusted_prices'
if not os.path.exists(adjust_price_dir):
    os.makedirs(adjust_price_dir)

if __name__ == '__main__':
    begin = datetime(2010, 3, 1)
    end = datetime.now()
    products = ['AP', 'CF', 'CJ', 'FG', 'IC', 'IF', 'IH', 'MA', 'OI', 'RM', 'SF', 'SM', 'SR', 'T', 'TA', 'ZC',
                'a', 'ag', 'al', 'au', 'b', 'bu', 'c', 'cs', 'cu', 'eb', 'eg', 'hc', 'i', 'j', 'jd', 'jm', 'l', 'lu',
                'm', 'ni', 'nr', 'p', 'pb', 'pg', 'pp', 'rb', 'ru', 'sn', 'sp', 'ss', 'v', 'y', 'zn']
    for prod in products:
        generate_multiple_price(prod, begin, end, multiple_price_dir)
        print(f'{prod} complete')
