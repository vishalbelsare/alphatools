import pandas as pd
from zipline.utils.paths import data_root
from zipline.utils.run_algo import loaders
#from alphatools.research import loaders
from zipline.pipeline.data import Column
from zipline.pipeline.data import DataSet
from zipline.pipeline.loaders.frame import DataFrameLoader

import os, sys, inspect
from os import path
from collections import OrderedDict
###old from zipline.utils.calendars import get_calendar
from trading_calendars import get_calendar
from datetime import datetime
import pytz
from pytz import timezone as _tz  # Python only does once, makes this portable.

#   Move to top of algo for better efficiency.

cur_folder    = os.path.realpath(os.path.abspath(os.path.split(inspect.getfile( inspect.currentframe() ))[0]))

###FUND_FRAME_FILE ="sharadar_with_sid-mcap-de-2020-03-27.pkl"
FUND_FRAME_FILE ="quandal_sharadar_sf1.pkl"


zipline_data_root_path = data_root()
fundementals_dir  = os.path.join(zipline_data_root_path,'fundem-sharadar-sf1')

#this_user_path    = os.path.expanduser("~")
#fundementals_dir  = os.path.join(this_user_path,'.zipline/data/fundem-sharadar-sf1')

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

trading_calendar = get_calendar('NYSE')
current_time  = pd.datetime.utcnow()
start_session = df.datekey.max()
end_session   = current_time
extend_sessions = trading_calendar.sessions_in_range(start_session, end_session)
print ("\nSF1 Table needs to extend sessions from:max datekey:{} tp  current date:{} ExtendRange:{}\n".format(start_session,end_session,extend_sessions))


df=df.reset_index() # Date is now a col, and index is default. Setup for below loaders

#df.groupby(['newidx', 'Code'], as_index=False)['val'].max().unstack()

sharadar_f1_top25 = [
#"revenue",
#"opex",
#"netinc",
#"equity",
#"debt",
#"ebitda",

#"revenueusd",
#"debtusd",
#"dps",
"fcf",
#"gp",
#"grossmargin",
#"netmargin",
#"ros",
#"payoutratio",
#"pe1",
#"ps1",
#"pb",

#"divyield",
#"currentratio",

"de",
'marketcap',
'debtnc',
"equityusd",

#"liabilities",
#"liabilitiesnc",
#"assets",
#"ev",
#"ebt",
#"ebit",
#"cashneq",
]
#,
#"ticker",
#"reportperiod",
#]

sharadar_tickers = [
"category", #type: str
"exchange", #type str
"isdelisted",#type str
]

fundy_frames = OrderedDict()
for name  in sharadar_f1_top25:
    print("Adding fundamental:{}".format(name))
    fundy_frames[name]         = df[['datekey',name, 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
    fundy_frames[name]         = fundy_frames[name].pivot_table(values=name, index='datekey', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
    fundy_frames[name].index   = pd.to_datetime(fundy_frames[name].index)
    fundy_frames[name].index   = fundy_frames[name].index.tz_localize('UTC')
    fundy_frames[name]         = fundy_frames[name].sort_index().fillna(method='ffill')

for name  in sharadar_tickers:
    print("Adding tickers:{}".format(name))
    fundy_frames[name]         = df[['datekey',name, 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
    #fundy_frames[name]         = fundy_frames[name].pivot_table(values=name, index='datekey', columns='sid', fill_value='NullCat', margins=False, aggfunc=lambda x: ' '.join(x), dropna=True, margins_name='All')
    fundy_frames[name]         = fundy_frames[name].pivot_table(values=name, index='datekey', columns='sid', fill_value=None, margins=False, aggfunc=lambda x: ' '.join(x), dropna=False, margins_name='All')
    fundy_frames[name].index   = pd.to_datetime(fundy_frames[name].index)
    fundy_frames[name].index   = fundy_frames[name].index.tz_localize('UTC')
    fundy_frames[name]         = fundy_frames[name].sort_index().fillna(axis=1, method='ffill').fillna(axis=1,method='bfill') # doesn't work for 2d.astype('category')
    #fundy_frames[name]         = fundy_frames[name].sort_index().fillna(method='ffill')
    #fundy_frames[name]         = fundy_frames[name].ffill()
    
    ###fundy_frames[name]         = fundy_frames[name].sort_index().fillna(method='ffill')

###MarketCap_frame         = df[['datekey','marketcap', 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
###MarketCap_frame = MarketCap_frame.pivot_table(values='marketcap', index='datekey', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
###MarketCap_frame.index   = pd.to_datetime(MarketCap_frame.index)
###MarketCap_frame.index   = MarketCap_frame.index.tz_localize('UTC')
###MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')

####MarketCap_frame = MarketCap_frame.unstack().sort_index()

####MarketCap_frame.columns = MarketCap_frame.columns.droplevel()
####MarketCap_frame.index   = pd.to_datetime(MarketCap_frame.index)
####MarketCap_frame.index   = MarketCap_frame.index.tz_localize('UTC')
####MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')


####MarketCap_frame         = df[['Date','marketcap', 'sid']].reset_index().set_index(['Date', 'sid'], append=True).sort_index().drop(columns=['index']).unstack().sort_index()
####MarketCap_frame.columns = MarketCap_frame.columns.droplevel()
####MarketCap_frame.index   = pd.to_datetime(MarketCap_frame.index)
####MarketCap_frame.index   = MarketCap_frame.index.tz_localize('UTC')
####MarketCap_frame         = MarketCap_frame.sort_index().fillna(method='ffill')



####DE_frame         = df[['Date','de', 'sid']]
####DE_frame.columns = DE_frame.columns.droplevel()
####DE_frame.index   = pd.to_datetime(DE_frame.index)
####DE_frame.index   = DE_frame.index.tz_localize('UTC')
####DE_frame         = DE_frame.sort_index().fillna(method='ffill')

####MarketCap_frame  = df[['marketcap', 'sid']].sort_index().fillna(method='ffill')
####DE_frame         = df[['de', 'sid']].sort_index().fillna(method='ffill')

###DE_frame         = df[['datekey','de', 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
###DE_frame = DE_frame.pivot_table(values='de', index='datekey', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
###DE_frame.index   = pd.to_datetime(DE_frame.index)
###DE_frame.index   = DE_frame.index.tz_localize('UTC')
###DE_frame         = DE_frame.sort_index().fillna(method='ffill')

###EUSD_frame         = df[['datekey','equityusd', 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
###EUSD_frame = EUSD_frame.pivot_table(values='equityusd', index='datekey', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
###EUSD_frame.index   = pd.to_datetime(EUSD_frame.index)
###EUSD_frame.index   = EUSD_frame.index.tz_localize('UTC')
###EUSD_frame         = EUSD_frame.sort_index().fillna(method='ffill')

###DNC_frame         = df[['datekey','debtnc', 'sid']].reset_index().set_index(['datekey', 'sid']).sort_index()
###DNC_frame = DNC_frame.pivot_table(values='debtnc', index='datekey', columns='sid', aggfunc='max', fill_value=None, margins=False, dropna=True, margins_name='All')
###DNC_frame.index   = pd.to_datetime(DNC_frame.index)
###DNC_frame.index   = DNC_frame.index.tz_localize('UTC')
###DNC_frame         = DNC_frame.sort_index().fillna(method='ffill')

# A DataSet with lots of columns.
class Fundamentals(DataSet):
    locals().update({
        name: Column(dtype=float)  for name in sharadar_f1_top25
    })
    locals().update({
            name: Column(dtype=object)  for name in sharadar_tickers
    })
    
#class Fundamentals(DataSet):
#    for name  in sharadar_f1_top25:
#        name = Column(dtype=float)

#class Fundamentals(DataSet):
    #DE = Column(dtype=float)
    #MarketCap = Column(dtype=float)
    #EUSD = Column(dtype=float)
    #DNC = Column(dtype=float)

# register the loaders
for name  in sharadar_f1_top25:
    loaders[Fundamentals.get_column(name)] = DataFrameLoader(Fundamentals.get_column(name), fundy_frames[name] )

for name  in sharadar_tickers:
    loaders[Fundamentals.get_column(name)] = DataFrameLoader(Fundamentals.get_column(name), fundy_frames[name] )

#loaders[Fundamentals.DE]        = DataFrameLoader(Fundamentals.DE, DE_frame)
#loaders[Fundamentals.MarketCap] = DataFrameLoader(Fundamentals.MarketCap, MarketCap_frame)
#loaders[Fundamentals.EUSD]      = DataFrameLoader(Fundamentals.EUSD, EUSD_frame)
#loaders[Fundamentals.DNC]       = DataFrameLoader(Fundamentals.DNC, DNC_frame)

df_loaders=loaders
