#!/usr/bin/python

import numpy as np
import pandas as pd
#from pandas.tseries.holiday import USFederalHolidayCalendar,AbstractHolidayCalendar
from holiday import USMarketHolidayCalendar
from pandas.tseries.offsets import CDay,Day,Week,MonthBegin,MonthEnd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
import re
import pickle
import sys
import os
import code
import logging

logger = logging.getLogger(__name__)

# References to the US Federal Government Holiday Calendar and current time.
calendar_us = USMarketHolidayCalendar()
bday_us     = CDay(calendar=calendar_us)
now         = pd.to_datetime('now').tz_localize('UTC')

# References to CBOE's historical futures data.
cboe_historical_base_url = 'http://cfe.cboe.com/Publish/ScheduledTask/MktData/datahouse'
cboe_base_millennium     = 2000
#                  J    F    M    A    M    J    J    A    S    O    N    D
#                  a    e    a    p    a    u    u    u    e    c    o    e
#                  n    b    r    r    y    n    l    g    p    t    v    c
month_code = ['', 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
#             0    1    2    3    4    5    6    7    8    9   10   11   12

# Time when CBOE updates historical futures data.
cboe_update_time_str = '10:00' # Chicago time
cboe_update_time     = pd.to_datetime('{:%Y-%m-%d} {}'.format(now, cboe_update_time_str)).\
        tz_localize('America/Chicago').tz_convert('UTC')

# Other CBOE information.
num_active_vx_contracts = 8

def fetch_vx_contracts(period):
    """Return a dataframe of VX contracts.

    period -- target timeframe.
    """
    # Resample period into months plus future months over which active contracts expire.
    months = pd.date_range(start=period[0], end=(period[-1]+num_active_vx_contracts*MonthBegin()), freq='MS')
    logger.debug('months =\n{}'.format(months))

    # Load VX contracts.
    vx_contracts = [fetch_vx_monthly_contract(d) for d in months]

    # Merge homogeneous dataframes (contracts) into a single dataframe, indexed by trading day.
    vx_contract_df               = pd.concat(vx_contracts).set_index('Trade Date', drop=False)

    # Exclude invalid entries and entries outside the target timeframe.
    vx_contract_df = vx_contract_df.loc[period]
    vx_contract_df = vx_contract_df.dropna()
    return(vx_contract_df)
#END: fetch_vx_contracts

def fetch_vx_monthly_contract(monthyear, cache=True, force_update=False, cache_dir='.data'):
    """Return historical data and expiration date from CBOE or local cache.

    monthyear -- contract's month and year of expiration.

    cache -- enable cache.

    force_update -- always update cache.

    cache_dir -- cache's base directory path.
    """
    contract_name = '({}){:%m/%Y}'.format(month_code[monthyear.month], monthyear)
    logger.debug('Fetching futures contract {}.'.format(contract_name))

    cache_path    = '{}/VX_{:%Y_%m}.p'.format(cache_dir, monthyear)
    vx_expdate    = get_vx_expiration_date(monthyear)

    try:
        # Setup cache directory
        os.mkdir(cache_dir)
    except FileExistsError:
        pass
    except OSError:
        # Disable cache if cache directory is inaccessible.
        cache = False

    try:
        # Load contract from cache.
        if(not cache):
            raise
        vx_contract = pickle.load(open(cache_path, 'rb'))
        if(force_update or not is_cboe_cache_current(vx_contract, vx_expdate, cache_path)):
            raise
        logger.debug('Retrieved VX contract {} from cache ({}).'.format(contract_name, cache_path))
    except:
        # Fallback to fetching from CBOE.
        try:
            # (example: CFE_F16_VX.csv for the January 2016 contract)
            vx_contract = pd.read_csv(
                '{}/CFE_{}{}_VX.csv'.format(cboe_historical_base_url, month_code[monthyear.month],
                    monthyear.year - cboe_base_millennium),
                header=1,
                names=['Trade Date','Futures','Open','High','Low','Close','Settle',
                    'Change','Total Volume','EFP','Open Interest'])
        except:
            logger.exception('Failed to download VX contract {} from CBOE.'.format(contract_name))
            raise

        try:
            # Parse dates (assuming MM/DD/YYYY format), set timezone to UTC, and reset to midnight.
            vx_contract['Trade Date'] = pd.to_datetime(vx_contract['Trade Date'],
                    format='%m/%d/%Y').apply(lambda x: x.tz_localize('UTC'))
            vx_contract['Trade Date'] = pd.DatetimeIndex(vx_contract['Trade Date']).normalize()
        except:
            logger.exception('Unexpected datetime format from CBOE.')
            raise

        # Discard entries at expiration and beyond.
        vx_contract = vx_contract[vx_contract['Trade Date'] < vx_expdate]

        logger.debug('Retrieved contract {} from CBOE.'.format(contract_name))

        try:
            if(cache):
                # Cache VX contract.
                pickle.dump(vx_contract, open(cache_path, 'wb'))
                logger.debug('Cached contract {} in ({}).'.format(contract_name, cache_path))
        except:
            logger.exception('Failed to cache VX contract {}.'.format(contract_name))

    vx_contract['Expiration Date'] = vx_expdate

    logger.debug('Sample trade date = {}'.format(vx_contract['Trade Date'][0]))
    logger.debug('vx_contract =\n{}'.format(vx_contract))
    return(vx_contract)
#END: fetch_vx_monthly_contract

def is_cboe_cache_current(contract, expdate, cache_path):
    """Test whether or not the contract's cache is up-to-date.

    contract -- contract dataframe.

    expdate -- contract's expiration date.

    cache_path -- file path to contract's cache.
    """
    is_current = True
    try:
        # Check if contract is expired or cache is up-to-date.
        current_datetime = cboe_update_time if(now >= cboe_update_time) else (cboe_update_time - bday_us)
        cache_last_date  = contract['Trade Date'].iloc[-1]
        if(cache_last_date < (expdate-bday_us) and current_datetime > (cache_last_date+2*bday_us)):
            logger.debug('Cache ({}) is out-of-date.'.format(cache_path))
            is_current = False
    except OSError:
        # Contract is not cached; therefore, cache is not up-to-date.
        is_current = False
    return(is_current)
#END: is_cache_current

def get_vx_expiration_date(monthyear):
    """Return the expiration date of a given VX contract.

    monthyear -- contract's month and year of expiration.
    """
    contract_name = '({}){:%m/%Y}'.format(month_code[monthyear.month], monthyear)

    # Compute the expiration date using rules from CBOE:
    #       http://cfe.cboe.com/products/vx-cboe-volatility-index-vix-futures/contract-specifications
    #       (section "Final Settlement Date")
    #   The final settlement date is the Wednesday 30 days prior to the third Friday of the calendar month
    #   following the month in which the contract expires. If that Wednesday or the Friday that is 30 days
    #   following that Wednesday is a CBOE holiday, then the final settlement date is the busisness day
    #   preceding that Wednesday.
    mp1                     = monthyear + MonthEnd()
    third_friday_of_mp1     = mp1 + 3*Week(weekday=4)
    expdate                 = third_friday_of_mp1 - 30*Day()
    if(not(is_business_day(third_friday_of_mp1) and is_business_day(expdate))):
        expdate = expdate - bday_us

    # Make it timezone-aware.
    if(expdate.tzinfo is None):
        expdate = expdate.tz_localize('UTC')
    else:
        expdate = expdate.tz_convert('UTC').normalize()

    logger.debug('Contract {} expires on {:%Y-%m-%d}.'.format(contract_name, expdate))
    return expdate
#END: get_vx_expiration_date

def is_business_day(date):
    """Test if date is a business day.

    date -- date of interest.
    """
    return(len(pd.date_range(start=date, end=date, freq=bday_us)) > 0)
#END: is_business_day

def count_business_days(start, end):
    """Count the number of business days between two dates.

    start -- first date.

    end -- second date.
    """
    mask          = pd.notnull(start) & pd.notnull(end)
    start         = start.values.astype('datetime64[D]')[mask]
    end           = end.values.astype('datetime64[D]')[mask]
    holidays      = calendar_us.holidays().values.astype('datetime64[D]')
    result        = np.empty(len(mask), dtype= float)
    result[mask]  = np.busday_count(start, end, holidays=holidays)
    result[~mask] = np.nan
    return(result)
#END: count_business_days

def test_plot():
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('cboe.test_plot.log', 'w')
    fh.setFormatter(fmt)
    con = logging.StreamHandler(sys.stdout)
    con.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(con)

    timeframe     = 2*200
    end_date      = (now - bday_us*(1+ (now < cboe_update_time) )).normalize()
    start_date    = end_date - timeframe*bday_us
    target_period = pd.date_range(start=start_date, end=end_date, freq=bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # Load VX contracts.
    vx_contract_df = fetch_vx_contracts(target_period)
    timeframe      = vx_contract_df.index.unique()

    # Create GroupBy objects.
    vx_td_gb = vx_contract_df.groupby('Trade Date')      # group by trading day
    vx_ed_gb = vx_contract_df.groupby('Expiration Date') # group by expiration day

    logger.debug('vx_td_gb =\n{}'.format(vx_td_gb))

    # Create continuous prior-month expiration date series, indexed by trading day.
    vx_expdate_s = pd.Series(vx_ed_gb.first().index) # get list (series) of expiration dates
    vx_pm_s      = pd.Series(
            [
                vx_expdate_s[ vx_expdate_s <= d ].iloc[-1]
                if(len(vx_expdate_s[ vx_expdate_s <= d ]) > 0)
                else None
                for d in timeframe
            ],
            index=timeframe
            )
    vx_pm_s = vx_pm_s.dropna() # exclude entries without a prior-month contract

    logger.debug('vx_expdate_s =\n{}'.format(vx_expdate_s))

    # Create continuous VX futures dataframes.
    vx_fm_df = vx_td_gb.nth(0) # front-month
    vx_bm_df = vx_td_gb.nth(1) # back-month

    logger.debug('vx_fm_df =\n{}'.format(vx_fm_df))

    # Create custom dataframes indexed by trading day.
    vx_st_df = pd.DataFrame(index=vx_pm_s.index) # short-term (front/back-month weighted)
    logger.debug('vx_pm_s.index =\n{}'.format(vx_pm_s.index))

    # Calculate short-term columns.
    vx_st_df['Prior-Month Expiration Date'] = vx_pm_s
    vx_st_df['Front-Month Settle']          = vx_fm_df['Settle']
    vx_st_df['Front-Month Expiration Date'] = vx_fm_df['Expiration Date']
    vx_st_df['Back-Month Settle']           = vx_bm_df['Settle']
    vx_st_df['Back-Month Expiration Date']  = vx_bm_df['Expiration Date']
    vx_st_df['Roll Period']                 = count_business_days(
            vx_st_df['Prior-Month Expiration Date'], vx_st_df['Front-Month Expiration Date'])
    vx_st_df['Days Till Rollover']          = (count_business_days(
            vx_st_df.index, vx_st_df['Front-Month Expiration Date']) - 1)
    vx_st_df['Front-Month Weight']          = vx_st_df['Days Till Rollover'] / vx_st_df['Roll Period']
    vx_st_df['Back-Month Weight']           = 1.0 - vx_st_df['Front-Month Weight']
    vx_st_df['STCMVF']                      =\
        vx_st_df['Front-Month Weight'] * vx_st_df['Front-Month Settle'] +\
        vx_st_df['Back-Month Weight'] * vx_st_df['Back-Month Settle']

    logger.debug('vx_st_df =\n{}'.format(vx_st_df[['Front-Month Expiration Date','Roll Period',
        'Days Till Rollover','Front-Month Weight','Back-Month Weight']]))

    # Fetch VIX daily quotes from Yahoo! Finance.
    vix_df = web.DataReader('^VIX', 'yahoo', start=vx_pm_s.index[0], end=vx_pm_s.index[-1])
    vix_df = vix_df.tz_localize('UTC') # make dates timezone-aware
    vx_st_df['VIX'] = vix_df['Adj Close']

    # Plot
    vx_st_df[['VIX','STCMVF']].plot()
    plt.savefig('st.png')

    # Drop into a Python shell with all definitions.
    code.interact(local=dict(globals(), **locals()))
#END: test_plot

if(__name__ == '__main__'):
    test_plot()
