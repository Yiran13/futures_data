import pandas as pd
instrument_list=['au','cu','m',
                 'BRENT-LAST','rb','hc','i','j',"jm",'IC', 'IF','DAX','NIKKEI','SP500','US2','US10','JGB','BUND']
path_list = ['multiple_prices','adjusted_prices']
for instrument in instrument_list:
    for path in path_list:
        data = pd.read_csv(f'{path}/{instrument}.csv')
        data.loc[:,'DATETIME'] = pd.to_datetime(data['DATETIME'])
        data = data.loc[data['DATETIME']>='2010']
        del data['Unnamed: 0']
        data.to_csv(f'{path}/{instrument}.csv',index=False)