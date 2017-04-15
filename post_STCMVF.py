#!/usr/bin/python
#
# Calculate the daily short-term VIX futures value and
# post value to StockTwits.

from credentials import st_access_token
import pandas as pd
#from pandas.tseries.holiday import USFederalHolidayCalendar
from holiday import USMarketHolidayCalendar
from pandas.tseries.offsets import CDay
import requests
import ssl
import re
import pickle
import sys
import logging
import logging.config

# Log setup
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('post_STCMVF')

# References to the US Federal Government Holiday Calendar and today's date.
calendar_us = USMarketHolidayCalendar()
bday_us     = CDay(calendar=calendar_us)
today       = pd.datetime.now().date()

# Is today a business day? If not, quit.
if(len(pd.DatetimeIndex(start=today, end=today, freq=bday_us)) == 0):
    logger.debug('Today (' + str(today) + ') is a non-workday. Aborting...')
    sys.exit()
logger.debug('Today (' + str(today) + ') is a workday. Proceeding...')

# Load persistent variables, prompt the user in case of missing variables.
try:
    (
            prior_expdate,
            prior_front_month_price,
            prior_back_month_price
            ) = pickle.load(open('data.p','rb'))
except (FileNotFoundError, TypeError):
    logger.exception('Failed to load "data.p". Please run "setup_data.py".')
    raise
except:
    logger.exception('Unexpected error:', sys.exc_info()[0])
    raise

logger.debug('prior_expdate           = ' + str(prior_expdate          ))
logger.debug('prior_front_month_price = ' + str(prior_front_month_price))
logger.debug('prior_back_month_price  = ' + str(prior_back_month_price ))

# Read today's settlement values of the VIX futures from CBOE.
#XXX: assumption that today's values have been posted at the time this script is executed
# Format:
#    Symbol         SettlementPrice
#    VX MM/DD/YYYY  *.***               <-- Front month
#    VX** ExpDate2  *.***               <-- Weekly 1
#    VX** ExpDate3  *.***               <-- Weekly 2
#    VX** ExpDate4  *.***               <-- Weekly 3
#    VX ExpDate5    *.***               <-- Back month
#    ...
try:
    vx_eod_values = pd.read_csv('http://www.cfe.cboe.com/data/DailyVXFuturesEODValues/DownloadFS.aspx',
            header=0, names=['Symbol', 'SettlementPrice'])
except: # fallback to HTML table
    try:
        cboe_tables = pd.read_html('http://cfe.cboe.com/data/dailyvxfutureseodvalues/default.aspx',
                match='Settlement Price', header=0)
        vx_eod_values = pd.DataFrame()
        vx_eod_values['Symbol']          = cboe_tables[0]['Symbol']
        vx_eod_values['SettlementPrice'] = cboe_tables[0]['Daily\n                                Settlement Price']
    except:
        logger.exception('Failed to download daily settlement values from CBOE.')
        raise

logger.debug('vx_eod_values =\n' + str(vx_eod_values))

# Grab the front and back month expirations and settlement prices.
p_monthly_expdate      = re.compile('VX \s*(.*)')
monthly_vx_eod_values  = vx_eod_values[
        vx_eod_values['Symbol'].map(lambda x: p_monthly_expdate.match(x) is not None)
        ]
try:
    front_month_eod_value = monthly_vx_eod_values.iloc[0]
    back_month_eod_value  = monthly_vx_eod_values.iloc[1]
except:
    logger.exception('Failed to find monthly contract settlement data.')
    raise
try:
    front_month_expdate    = pd.to_datetime(
            p_monthly_expdate.match(front_month_eod_value['Symbol']).group(1),
            format='%m/%d/%Y').date()
    back_month_expdate     = pd.to_datetime(
            p_monthly_expdate.match(back_month_eod_value['Symbol']).group(1),
            format='%m/%d/%Y').date()
except:
    logger.exception('Failed to read monthly contract expiration dates.')
    raise
front_month_price = front_month_eod_value['SettlementPrice']
back_month_price  = back_month_eod_value['SettlementPrice']

logger.debug('monthly_vx_eod_values =\n' + str(monthly_vx_eod_values))
logger.debug('front_month_expdate = ' + str(front_month_expdate))
logger.debug('back_month_expdate  = ' + str(back_month_expdate ))
logger.debug('front_month_price   = ' + str(front_month_price  ))
logger.debug('back_month_price    = ' + str(back_month_price   ))

# Calculate the prior front and back month weights.
#   calculate the number of trading sessions from prior expiration (inclusive) to front-month expiration (exclusive)
prior_period_dates = pd.DatetimeIndex(start=prior_expdate, end=(front_month_expdate-bday_us), freq=bday_us)
prior_period       = len(prior_period_dates)
#   calculate the number of trading sessions from last session (inclusive) to front-month expiration (exclusive)
prior_leftover_dates = pd.DatetimeIndex(start=(today-bday_us), end=(front_month_expdate-bday_us), freq=bday_us)
prior_days_left      = len(prior_leftover_dates)
#   now the prior weights
try:
    prior_front_month_weight = float(prior_days_left) / prior_period
except ZeroDivisionError:
    logger.exception('No workdays from prior expiration (' + str(prior_expdate) +
            ') to the front month contract (' + str(front_month_expdate) + ').')
    raise
prior_back_month_weight  = 1.0 - prior_front_month_weight

logger.debug('Dates in prior period:\n' + str(prior_period_dates))
logger.debug('Dates from last session:\n'  + str(prior_leftover_dates    ))
logger.debug('prior_period             = ' + str(prior_period            ))
logger.debug('prior_days_left          = ' + str(prior_days_left         ))
logger.debug('prior_front_month_weight = ' + str(prior_front_month_weight))
logger.debug('prior_back_month_weight  = ' + str(prior_back_month_weight ))

# If today is expiration, update the prior expiration to front-month's expiration
#   assume the back-month as the new front-month.
if(today == front_month_expdate):
    logger.debug('Today (' + str(today) + ') is expiration day.')
    prior_expdate = front_month_expdate
    logger.debug('Updated prior expiration to ' + str(front_month_expdate))
    front_month_expdate = back_month_expdate
    front_month_price   = back_month_price
    try:
        back_month_expdate  = pd.to_datetime(
            p_monthly_expdate.match(monthly_vx_eod_values.iloc[2]['Symbol']).group(1),
            format='%m/%d/%Y').date()
    except:
        logger.exception('Failed to update back month expiration date.')
    back_month_price    = monthly_vx_eod_values.iloc[2]['SettlementPrice']

    logger.debug('new front_month_expdate = ' + str(front_month_expdate))
    logger.debug('new back_month_expdate  = ' + str(back_month_expdate ))
    logger.debug('new front_month_price   = ' + str(front_month_price  ))
    logger.debug('new back_month_price    = ' + str(back_month_price   ))

# Calculate front and back month weights.
#   calculate the number of trading sessions from prior expiration (inclusive) to front-month expiration (exclusive)
period_dates = pd.DatetimeIndex(start=prior_expdate, end=(front_month_expdate-bday_us), freq=bday_us)
period       = len(period_dates)
#   calculate the number of trading sessions from today (inclusive) to front-month expiration (exclusive)
leftover_dates = pd.DatetimeIndex(start=today, end=(front_month_expdate-bday_us), freq=bday_us)
days_left      = len(leftover_dates)
#   now the weights
try:
    front_month_weight = float(days_left) / period
except ZeroDivisionError:
    logger.exception('No workdays from prior expiration (' + str(prior_expdate) +
            ') to the front month contract (' + str(front_month_expdate) + ').')
    raise
back_month_weight  = 1.0 - front_month_weight

logger.debug('Dates in period:\n' + str(period_dates  ))
logger.debug('Dates from now:\n'  + str(leftover_dates))
logger.debug('period             = ' + str(period            ))
logger.debug('days_left          = ' + str(days_left         ))
logger.debug('front_month_weight = ' + str(front_month_weight))
logger.debug('back_month_weight  = ' + str(back_month_weight ))

# Calculate today's Short-Term Constant-Maturity VIX Futures (STCMVF) settlement and percent change
#   from previous session.
stcmvf_prior = prior_front_month_weight * prior_front_month_price + prior_back_month_weight * prior_back_month_price
stcmvf_today = front_month_weight * front_month_price + back_month_weight * back_month_price
stcmvf_percent = (stcmvf_today / stcmvf_prior) - 1.0

logger.debug('stcmvf_prior   = ' + str(stcmvf_prior  ))
logger.debug('stcmvf_today   = ' + str(stcmvf_today  ))
logger.debug('stcmvf_percent = ' + str(stcmvf_percent))

# Post to StockTwits.
st_message = '$VXX $XIV $SVXY $TVIX $UVXY Short-term constant-maturity VIX futures settled @ ' +\
        '{:.3f} ({:+.1%}).'.format(stcmvf_today, stcmvf_percent)
st_payload      = {'access_token':st_access_token, 'body':st_message}

logger.debug('st_payload = ' + str(st_payload))

try:
    r = requests.post('https://api.stocktwits.com/api/2/messages/create.json', data=st_payload)
    r = r.json()
    logger.debug('Response from StockTwits = ' + str(r))
    status = r['response']['status']
    logger.debug('Status from StockTwits = ' + str(status))
    if(status != 200):
        raise Exception('Received invalid response from StockTwits: ' + str(status) + ': ' + str(r))
except:
    logger.exception('Failed to post to StockTwits.')
    raise

logger.info('Posted message: ' + st_message)

# Update prior front/back month prices.
prior_front_month_price = front_month_price
prior_back_month_price  = back_month_price

# Save updated persistent variables.
try:
    pickle.dump((
        prior_expdate,
        prior_front_month_price,
        prior_back_month_price
        ), open('data.p','wb'))
except:
    logger.exception('Failed to save state.')
    raise
