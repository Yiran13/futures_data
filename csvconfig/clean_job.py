import pandas as pd
data = pd.read_csv('instrumentconfig.csv')
data.loc[:,'Currency']='CNH'
data[['Instrument','Pointsize','Percentage','PerBlock','PerTrade','AssetClass','Currency','Description','Slippage']].to_csv('instrumentconfig.csv',index=False)