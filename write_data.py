from private.futures.prepare_data import multiple_price_dir, adjust_price_dir
from sysdata.csv.csv_instrument_data import csvFuturesInstrumentData
from sysdata.mongodb.mongo_futures_instruments import mongoFuturesInstrumentData
from sysinit.futures.adjustedprices_from_mongo_multiple_to_mongo import process_adjusted_prices_instruments

from sysinit.futures.multipleprices_from_arcticprices_and_csv_calendars_to_arctic import \
    process_multiple_prices_instruments

instrument_list=['au','cu','m','BRENT-LAST','rb','hc','i','j',"jm",'IC', 'IF','DAX','NIKKEI','SP500','T','US2','US10','JGB','BUND']

# instrument_list = ['AP', 'CF', 'CJ', 'FG', 'IC', 'IF', 'IH', 'MA', 'OI', 'RM', 'SF', 'SM', 'SR', 'T', 'TA', 'ZC',
#                    'a', 'ag', 'al', 'au', 'b', 'bu', 'c', 'cs', 'cu', 'eb', 'eg', 'hc', 'i', 'j', 'jd', 'jm', 'l',
#                    'lu', 'm', 'ni', 'nr', 'p', 'pb', 'pg', 'pp', 'rb', 'ru', 'sn', 'sp', 'ss', 'v', 'y', 'zn']

# step1: 将instrument的信息写入到mongodb数据库
instrument_config_dir = "private.futures.csvconfig"
data_in = csvFuturesInstrumentData(datapath=instrument_config_dir)
data_out = mongoFuturesInstrumentData()
print(data_in)
for instrument_code in instrument_list:
    instrument_object = data_in.get_instrument_data(instrument_code)
    # 删除掉原有数据
    data_out.delete_instrument_data(instrument_code, are_you_sure=True)
    # 重新写入新数据
    data_out.add_instrument_data(instrument_object)
    instrument_added = data_out.get_instrument_data(instrument_code)
    print("Added %s to %s" % (instrument_added.instrument_code, data_out))

# step2：读取本地生成的multiple_price,导入mongodb数据库
csv_multiple_data_path = f'{multiple_price_dir}'
process_multiple_prices_instruments(instruments=instrument_list, csv_multiple_data_path=csv_multiple_data_path)

# step3: 生成adjusted_price
csv_adj_data_path = f'private.futures.{adjust_price_dir}'
process_adjusted_prices_instruments(instruments=instrument_list, csv_adj_data_path=csv_adj_data_path)
