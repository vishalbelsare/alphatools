import pandas as pd
from zipline.utils.run_algo import loaders
#from alphatools.research import loaders
from zipline.pipeline.data import Column
from zipline.pipeline.data import DataSet
from zipline.pipeline.loaders.frame import DataFrameLoader

import os, sys, inspect
from os import path

cur_folder    = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))

###FUND_FRAME_FILE ="sharadar_with_sid-mcap-de-2020-03-27.pkl"
FUND_FRAME_FILE ="quandal_sharadar_sf1.pkl"
fundementals_dir  = '/home/ubuntu/.zipline/data/fundem-sharadar-sf1'

candidates = os.listdir(fundementals_dir)
candidates.sort()
latest_ingest = candidates[-1] #Latest is last one
print(candidates)
###enac_fundamentals_pkl = os.path.realpath(os.path.abspath(os.path.join(cur_folder, FUND_FRAME_FILE)))
enac_fundamentals_pkl = os.path.realpath(os.path.abspath(os.path.join(fundementals_dir, latest_ingest, FUND_FRAME_FILE)))


if cur_folder not in sys.path:
    sys.path.insert(0, cur_folder)
print ("cur_folder=", cur_folder)
print ("enac_fundamentals_pkl=", enac_fundamentals_pkl)
df = pd.read_pickle(enac_fundamentals_pkl)

non_bundle_assets=(df.sid==-1)
df=df[~(non_bundle_assets)]
print("Non-sid Fund Count={}".format(non_bundle_assets.value_counts()))
print("Non-sid Fund TotalCount={}".format(non_bundle_assets.sum()))

df=df.reset_index() # Date is now a col, and index is default. Setup for below loaders

#df.groupby(['newidx', 'Code'], as_index=False)['val'].max().unstack()

MarketCap_frame         = df[['Date','marketcap', 'sid']].reset_index().set_index(['Date', 'sid']).sort_index().drop(columns=['index'])
MarketCap_frame = MarketCap_frame.pivot_table(values='marketcap', index='Date', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')


#MarketCap_frame = MarketCap_frame.unstack().sort_index()

#MarketCap_frame.columns = MarketCap_frame.columns.droplevel()
#MarketCap_frame.index   = pd.to_datetime(MarketCap_frame.index)
#MarketCap_frame.index   = MarketCap_frame.index.tz_localize('UTC')
#MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')


#MarketCap_frame         = df[['Date','marketcap', 'sid']].reset_index().set_index(['Date', 'sid'], append=True).sort_index().drop(columns=['index']).unstack().sort_index()
#MarketCap_frame.columns = MarketCap_frame.columns.droplevel()
#MarketCap_frame.index   = pd.to_datetime(MarketCap_frame.index)
#MarketCap_frame.index   = MarketCap_frame.index.tz_localize('UTC')
#MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')



#DE_frame         = df[['Date','de', 'sid']]
#DE_frame.columns = DE_frame.columns.droplevel()
#DE_frame.index   = pd.to_datetime(DE_frame.index)
#DE_frame.index   = DE_frame.index.tz_localize('UTC')
#DE_frame         = DE_frame.sort_index().fillna(method='ffill')

#MarketCap_frame  = df[['marketcap', 'sid']].sort_index().fillna(method='ffill')
#DE_frame         = df[['de', 'sid']].sort_index().fillna(method='ffill')

DE_frame         = df[['Date','de', 'sid']].reset_index().set_index(['Date', 'sid']).sort_index().drop(columns=['index'])
DE_frame = DE_frame.pivot_table(values='de', index='Date', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
DE_frame         = DE_frame.sort_index().fillna(method='ffill')


class Fundamentals(DataSet):
    DE = Column(dtype=float)
    MarketCap = Column(dtype=float)

# register the loaders
loaders[Fundamentals.DE] = DataFrameLoader(Fundamentals.DE, DE_frame)
loaders[Fundamentals.MarketCap] = DataFrameLoader(Fundamentals.MarketCap, MarketCap_frame)
df_loaders=loaders
