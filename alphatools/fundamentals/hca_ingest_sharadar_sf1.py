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

from zipline.data import bundles



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

def download_url_to_targetfile(url, targetfile="/tmp/tartgetfile"):
  """
  Download data from a URL, writing a file in target dir.

  Parameters
  ----------
  url : str
      A URL that can be understood by ``requests.get``.

  Returns
  -------
  data : BytesIO
      A BytesIO containing the downloaded data.
  """

  resp = requests.get(url, timeout=50)
  resp.raise_for_status()
  with open(targetfile, 'wb') as f:
     f.write(resp.content)

  #return io.BytesIO(resp.content)
  return resp.status_code



api_key = 'FbEx4ddtmMx1-WkAvZVt' # enter your api key, it can be found in your Quandl account here: https://www.quandl.com/account/profile
table = 'SF1' # enter the Sharadar table you would like to retrieve
destFileRef = 'SF1_download2.csv.zip' # enter the destination that you would like the retrieved data to be saved to
#csv url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.csv?qopts.export=true&api_key=%s' % (table, api_key) # optionally add parameters to the url to filter the data retrieved, as described in the associated table's documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started
#json
url = 'https://www.quandl.com/api/v3/datatables/SHARADAR/%s.json?qopts.export=true&api_key=%s' % (table, api_key) # optionally add parameters to the url to filter the data retrieved, as described in the associated table's documentation, eg here: https://www.quandl.com/databases/SF1/documentation/getting-started

#version = sys.version.split(' ')[0]
#if version < '3':
  #import urllib2
  #fn = urllib2.urlopen
#else:
  #import urllib
  #fn = urllib.request.urlopen

valid = ['fresh','regenerating']
invalid = ['generating']
status = ''

while status not in valid:
  #Dict = json.loads(fn(url).read())

  Dict = json.loads(download_without_progress(url).read())

  last_refreshed_time = Dict['datatable_bulk_download']['datatable']['last_refreshed_time']
  status = Dict['datatable_bulk_download']['file']['status']
  link = Dict['datatable_bulk_download']['file']['link']

  from datetime import datetime

  date_string = last_refreshed_time
  last_refreshed_time_dt = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S %Z')
  last_refreshed_time_dir = datetime.strftime(last_refreshed_time_dt, '%Y-%m-%dT%H;%M;%S')

  print(last_refreshed_time_dir)


  print(status)
  if status not in valid:
    time.sleep(60)

print('fetching from %s' % link)
fundementals_dir  = '/home/ubuntu/.zipline/data/fundem-sharadar-sf1'
fundem_target_dir = os.path.join(fundementals_dir, last_refreshed_time_dir)

if not os.path.exists(fundem_target_dir):
  os.makedirs(fundem_target_dir)

fundem_target_file = os.path.join(fundem_target_dir, "SF1_table.zip")
zipl= (download_url_to_targetfile(link, fundem_target_file)) #Byte Stream
print("Done:SF1 scrape: last_refreshed_time:{} target_dir:{}".format(last_refreshed_time, fundem_target_file))

fundem_csv = None
with zipfile.ZipFile(fundem_target_file, 'r') as fundy_zip:
    fundem_csv = fundy_zip.infolist()[0]
    fundy_zip.extractall(fundem_target_dir)

print("fundem_csv={}".format( fundem_csv))
print("End: ingest quandl-sharadar-SF1-table")
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
df = pd.read_csv(fundem_target_file)#, nrows=1000)

print(df.head())
print(df.describe())
df=df[(df.dimension=='ARQ')] # Only take As-Reported-Quarterly (ARQ) dimension
###df.loc[:,'Date'] = pd.to_datetime(df.calendardate)
###df

#df['sid'] = 0 #original: np.nan
###df.set_index('Date', inplace=True)
df.info()
###if df.index.tzinfo is None:
###  df.index = df.index.tz_localize('UTC')
###dates = df.index.unique()
###print("NumberOfDates={} Dates={}".format(len(dates),dates))

current_time  = pd.datetime.utcnow()
start_session = df.datekey.max()
end_session   = current_time
extend_sessions = trading_calendar.sessions_in_range(start_session, end_session)
print ("\nSF1 Table needs to extend sessions from:max datekey:{} tp  current date:{} ExtendRange:{}\n".format(start_session,end_session,extend_sessions))

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


# Main computation of linking Sharadar SF1 Fundamentals Tabe to
df['sid'] = df.apply(lambda x:get_sid(x, pd.to_datetime(x.datekey).tz_localize('US/Eastern')), axis=1)
#df = df.assign(sid=Sid_col.values)

fund_data_persistent_store_file = os.path.join(fundem_target_dir, "quandal_sharadar_sf1.pkl")

df.to_pickle(fund_data_persistent_store_file)
#Final sanity check
print(df.info())
#[type(df[x][0]) for x in df.columns] #dtypes: float64(105), int64(1), object(6)    NOTE: All objects are strings. the sid column is int64, with 0 encoding NA
print([x for x in df.columns])
df.head()


print("end--ingest SF1")
