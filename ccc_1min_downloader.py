import os
import gzip
import json
import s3fs
import shutil
import requests
import logging
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class DownloadError(Exception):
    pass

def ccc_1min_bins_for_date(exch, pair, date):
    if isinstance(date, str):
        date = pd.to_datetime(date)
    to_ts = int((date + pd.Timedelta(days=1)).timestamp())
    fsym, tsym = pair.split('/')
    url = f'https://min-api.cryptocompare.com/data/histominute?fsym={fsym}&tsym={tsym}&e={exch}&toTs={to_ts}'
    logging.info(url)
    resp = requests.get(url).json()
    
    if resp['Response'] == 'Error':
        if resp['Message'].startswith('e param is not valid the market'):
            logger.warning(f"Warning - {resp['Message']} (pair = {pair}, exch = {exch}, date = {date})")
            return
        else:
            raise DownloadError(f"Error - {resp['Message']} (pair = {pair}, exch = {exch}, date = {date})")
    
    one_min_df = pd.DataFrame(resp['Data']).assign(time= lambda x: pd.to_datetime(x['time'], unit='s'))
    return one_min_df[:-1]


def update_1min_data(cfg):
    fs = s3fs.S3FileSystem()
    last_date = (pd.Timestamp.today().floor('1D') - pd.Timedelta(days=6))
    if fs.exists(cfg['s3path']):
        dirs = sorted(fs.ls(cfg['s3path']))
        if len(dirs) > 0:
            last_date = pd.to_datetime(dirs[-1].split('/date=')[-1])

    curr_date = pd.Timestamp.today().floor('1D').date()
    dates = pd.date_range(last_date + pd.Timedelta(days=1), curr_date, closed='left')
    for date in dates:
        tmpfile = f'/tmp/allpairs_{curr_date}.csv'
        with open(tmpfile, mode='a') as f:
            for exch, pair in cfg['pairs']:
                df = ccc_1min_bins_for_date(exch, pair, date)
                if df is not None:
                    df.assign(exchange=exch, pair=pair).to_csv(f, index=False)

        with open(tmpfile, 'rb') as f_in:
            with gzip.open(f'{tmpfile}.gz', 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
        fs.put(f'{tmpfile}.gz', cfg['s3path'] + f'/date={date.date()}/allpairs_1min_{date.date()}.csv.gz')

def download(cfgpath):
    fs = s3fs.S3FileSystem()
    with fs.open(cfgpath) as f:
        config = json.load(f) 
        update_1min_data(config)

