import json
import sys, os
import time
import zipfile
import io
import requests
import pandas as pd
###old from zipline.utils.calendars import get_calendar
from trading_calendars import get_calendar
from datetime import datetime
import pytz
from pytz import timezone as _tz  # Python only does once, makes this portable.
          #   Move to top of algo for better efficiency.
from dotenv import load_dotenv
          
from zipline.utils.paths import data_root
from zipline.data import bundles

def load_env_file():
  # Expecting, e.g.:
  # IB_ACCOUNTS=DU1234567,U9876543,DU38383838,U92929292
  #
  # for Sharadar ingestion
  # QUANDL_API_KEY=bEx90ss9xlad-sj9clsl
  fname = os.environ.get('HCA_ENV_FILE', "")
  if not fname:
    msg = "No env variable HCA_ENV_FILE, please set."
    print(msg)
    #st.write(msg)
    sys.exit()
  load_dotenv(dotenv_path=fname)
  success = True
  msg = ""
  for k in ["ZIPLINE_ROOT", "QUANDL_API_KEY"]:
    if k not in os.environ.keys():
      msg += f"Please set value of {k} in {fname}\n"
      success = False
  print(msg)
  if success:
    print(f"Successfully loaded config file {fname}!")
  else:
    #st.write(msg)
    sys.exit()


def download_without_progress(url):
  """
  Download data from a URL, returning a BytesIO containing the loaded data.

  Parameters
  ----------
  url : str
      A URL that can be understood by ``requests.get``.

  Returns
  -------
  data : io.BytesIO
      A io.BytesIO containing the downloaded data.
  """
  resp = requests.get(url)
  resp.raise_for_status()
  return io.BytesIO(resp.content)

def download_url_to_targetfile(url, targetfile_parm="/tmp/tartgetfile", table_parm='SF1'):
  """
  Download data from a URL, writing a file in target dir.

  Parameters
  ----------
  url : str
      A URL that can be understood by ``requests.get``.

  table : str
      A Sharadar table name.

  Returns
  -------
  data : BytesIO
      A BytesIO containing the downloaded data.
  """
  targetfile = targetfile_parm
  table = table_parm
  
  resp = requests.get(url, timeout=50)
  resp.raise_for_status()
  with open(targetfile, 'wb') as f:
     f.write(resp.content)

  #return io.BytesIO(resp.content)
  return resp.status_code




### Start: Sharadar table process code:
load_env_file()
api_key = os.environ.get('QUANDL_API_KEY')
if api_key is None:
  raise ValueError(
        "Please set your QUANDL_API_KEY environment variable and retry."
      )


#api_key = 'FbEx4ddtmMx1-WkAvZVt' # enter your api key, it can be found in your Quandl account here: https://www.quandl.com/account/profile
#table = SF1' # enter the Sharadar table you would like to retrieve
#csv url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.csv?qopts.export=true&api_key=%s' % (table, api_key) # optionally add parameters to the url to filter the data retrieved, as described in the associated table's documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started

tablef = 'SF1'
tablet = 'TICKERS'

#json
urlf = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s' % (tablef, api_key) # optionally add parameters to the url to filter the data retrieved, as described in the associated table's documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started
urlt = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s' % (tablet, api_key) # optionally add parameters to the url to filter the data retrieved, as described in the associated table's documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started

#version = sys.version.split(' ')[0]
#if version < '3':
  #import urllib2
  #fn = urllib2.urlopen
#else:
  #import urllib
  #fn = urllib.request.urlopen

valid = ['fresh','regenerating']
invalid = ['generating']
statusf = statust = ''

while (statusf not in valid) or (statust not in valid):
  #Dict = json.loads(fn(url).read())

  Dictf = json.loads(download_without_progress(urlf).read())
  Dictt = json.loads(download_without_progress(urlt).read())

  last_refreshed_timef = Dictf['datatable_bulk_download']['datatable']['last_refreshed_time']
  statusf = Dictf['datatable_bulk_download']['file']['status']
  linkf = Dictf['datatable_bulk_download']['file']['link']

  last_refreshed_timet = Dictt['datatable_bulk_download']['datatable']['last_refreshed_time']
  statust = Dictt['datatable_bulk_download']['file']['status']
  linkt = Dictt['datatable_bulk_download']['file']['link']

  from datetime import datetime

  date_string = last_refreshed_timef
  last_refreshed_time_dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %Z')
  last_refreshed_time_dir = datetime.strftime(last_refreshed_time_dt, '%Y-%m-%dT%H;%M;%S')

  print(last_refreshed_time_dir)


  print(statusf)
  if statusf not in valid:
    time.sleep(60)

print('fetching from %s' % linkf)
      
this_user_path = os.path.expanduser("~")
zipline_data_root_path = data_root()
fundementals_dir  = os.path.join(zipline_data_root_path,'fundem-sharadar-sf1')
#fundementals_dir  = os.path.join(this_user_path,'.zipline/data/fundem-sharadar-sf1')

fundem_target_dir = os.path.join(fundementals_dir, last_refreshed_time_dir)

if not os.path.exists(fundem_target_dir):
  os.makedirs(fundem_target_dir)

fundem_target_filef = os.path.join(fundem_target_dir, tablef + "_table.zip")
fundem_target_filet = os.path.join(fundem_target_dir, tablet + "_table.zip")

ziplf= (download_url_to_targetfile(linkf, fundem_target_filef, tablef)) #Byte Stream
ziplt= (download_url_to_targetfile(linkt, fundem_target_filet, tablet)) #Byte Stream
print("Done: {} scrape: last_refreshed_timef:{} target_dir:{}".format(tablef, last_refreshed_timef, fundem_target_filef))
print("Done: {} scrape: last_refreshed_timet:{} target_dir:{}".format(tablet, last_refreshed_timet, fundem_target_filet))

fundem_csv = None
with zipfile.ZipFile(fundem_target_filef, 'r') as fundy_zip:
    fundem_csv = fundy_zip.infolist()[0]
    fundy_zip.extractall(fundem_target_dir)
    print("fundem_csv={}".format( fundem_csv))
    print("End: ingest quandl-sharadar-" + tablef + "-table")

    
fundem_csv = None
with zipfile.ZipFile(fundem_target_filet, 'r') as fundy_zip:
    fundem_csv = fundy_zip.infolist()[0]
    fundy_zip.extractall(fundem_target_dir)
    print("fundem_csv={}".format( fundem_csv))
    print("End: ingest quandl-sharadar-" + tablet + "-table")
##zipString = (download_without_progress(link).read()) #Byte Stream
####zipString = (fn(link).read()) #Byte Stream
##zipString = str(fn(link).read()) #Str Stream

#z = zipfile.ZipFile(io.BytesIO(zipString))

## The following three lines are alternatives. Use one of them
## according to your need:
##foo = z.read('foo.txt')        # Reads the data from "foo.txt"
##foo2 = z.read(z.infolist()[0]) # Reads the data from the first file
#z.extractall()                 # Copies foo.txt to the filesystem
#z.close()

trading_calendar = get_calendar('NYSE')
###Old-orig bundle_data = bundles.load('quandl')
bundle_data = bundles.load('sharadar-prices')


# Fundementals from Quandl stored in a <name>.csv.zip file on local disk now(future place :AWS S3)
dff = pd.read_csv(fundem_target_filef)#, nrows=1000)
dft = pd.read_csv(fundem_target_filet)#, nrows=1000)

#print(dff.head())
#print(dff.describe())


dff=dff[(dff.dimension=='ARQ')] # Only take As-Reported-Quarterly (ARQ) dimension
dft=dft[(dft.table=='SEP')] # Only take SharadarEquityPrices table

###df.loc[:,'Date'] = pd.to_datetime(df.calendardate)
###df

#df['sid'] = 0 #original: np.nan
###df.set_index('Date', inplace=True)
#dff.info()
###if df.index.tzinfo is None:
###  df.index = df.index.tz_localize('UTC')
###dates = df.index.unique()
###print("NumberOfDates={} Dates={}".format(len(dates),dates))




current_time  = pd.datetime.utcnow()
start_session = dff.datekey.max()
end_session   = current_time
extend_sessions = trading_calendar.sessions_in_range(start_session, end_session)
print ("\n {} Table needs to extend sessions from:max datekey:{} tp  current date:{} ExtendRange:{}\n".format(tablef, start_session,end_session,extend_sessions))

def get_sid(row, day):
  ticker = row.ticker
  #day = pd.to_datetime(row.datekey).tz_localize('US/Eastern')
  #day = pd.to_datetime(row.calendardate).tz_localize('US/Eastern')
  ###print("row={} --- rowday={}".format(row,day))
  this_ticker = None
  try:
    this_ticker = bundle_data.asset_finder.lookup_symbol(ticker, as_of_date= day)
    this_sid = this_ticker.sid
    #print("Good:Date={} ticker = {} result={} this_sid={}".format(day, ticker, this_ticker, this_sid))

  except:
    #this_sid = np.nan
    this_sid = -1
    #print("Bad:Date={} ticker = {} result={} this_sid={}".format(day, ticker, this_ticker, this_sid))
  return this_sid

def get_cat(row, day, dft):
  ticker = row.ticker
  #DomComStk_lst= [
    #'Domestic Common Stock',
         ##'ADR Common Stock',
   #'Domestic Common Stock Primary Class',
         ##'Canadian Common Stock',
         ##'ADR Common Stock Primary Class',
         ##'Canadian Common Stock Primary Class',
         ##'Domestic Common Stock Secondary Class', 'Domestic Stock Warrant',
         ##'Domestic Preferred Stock', 'ADR Stock Warrant',
         ##'ADR Preferred Stock', 'ADR Common Stock Secondary Class',
         ##'Canadian Stock Warrant', 'Canadian Preferred Stock', nan, 'ETF',
         ##'CEF', 'ETN', 'ETD', 'IDX'
  #]  
  #day = pd.to_datetime(row.datekey).tz_localize('US/Eastern')
  #day = pd.to_datetime(row.calendardate).tz_localize('US/Eastern')
  #print("row={} --- rowday={}".format(row,day))
  this_cat = None
  this_ticker = None
  try:
    this_ticker = bundle_data.asset_finder.lookup_symbol(ticker, as_of_date= day)
    this_sid = this_ticker.sid
    #melt = melt.loc[melt['col'] == melt['variable'], 'value']
    
    this_cat = dft[dft.ticker==ticker].category.values[0] #str type
    #print("Good:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))

  except:
    this_sid = -1
    this_cat = None # empty string
    #print("Bad:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))
  return this_cat

def get_exchange(row, day, dft):
  ticker = row.ticker
  this_cat = None
  this_ticker = None
  try:
    this_ticker = bundle_data.asset_finder.lookup_symbol(ticker, as_of_date= day)
    this_sid = this_ticker.sid
    #melt = melt.loc[melt['col'] == melt['variable'], 'value']
    
    this_cat = dft[dft.ticker==ticker].exchange.values[0] #str type
    #print("Good:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))

  except:
    this_sid = -1
    this_cat = None # empty string
    #print("Bad:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))
  return this_cat

def get_isdelisted(row, day, dft):
  ticker = row.ticker
  this_cat = None
  this_ticker = None
  try:
    this_ticker = bundle_data.asset_finder.lookup_symbol(ticker, as_of_date= day)
    this_sid = this_ticker.sid
    #melt = melt.loc[melt['col'] == melt['variable'], 'value']
    
    this_cat = dft[dft.ticker==ticker].isdelisted.values[0] #str type
    #print("Good:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))

  except:
    this_sid = -1
    this_cat = None # empty string
    #print("Bad:Date={} ticker = {} cat={} this_sid={}".format(day, ticker, this_cat, this_sid))
  return this_cat


# Main computation of linking Sharadar SF1 Fundamentals Table to
dff['sid'] = dff.apply(lambda x:get_sid(x, pd.to_datetime(x.datekey).tz_localize('US/Eastern')), axis=1)
#df = df.assign(sid=Sid_col.values)

dff['category']=dff.apply(lambda x:get_cat(x, pd.to_datetime(x.datekey).tz_localize('US/Eastern'), dft), axis=1)
dff['exchange']=dff.apply(lambda x:get_exchange(x, pd.to_datetime(x.datekey).tz_localize('US/Eastern'), dft), axis=1)
dff['isdelisted']=dff.apply(lambda x:get_isdelisted(x, pd.to_datetime(x.datekey).tz_localize('US/Eastern'), dft), axis=1)

fund_data_persistent_store_file = os.path.join(fundem_target_dir, "quandal_sharadar_sf1.pkl")

dff.to_pickle(fund_data_persistent_store_file)
#Final sanity check
print(dff.info())
#[type(df[x][0]) for x in df.columns] #dtypes: float64(105), int64(1), object(6)    NOTE: All objects are strings. the sid column is int64, with 0 encoding NA
print([x for x in dff.columns])
dff.head()


print("end--ingest SF1")
