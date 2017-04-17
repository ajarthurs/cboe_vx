#!/usr/bin/python

"""Read and process futures data from CBOE."""

import numpy as np
import pandas as pd
#from pandas.tseries.holiday import USFederalHolidayCalendar,AbstractHolidayCalendar
from holiday import USMarketHolidayCalendar
from pandas.tseries.offsets import CDay,Day,Week,MonthBegin,MonthEnd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
import calendar
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
cboe_historical_update_time_str = '10:00' # Chicago time
cboe_historical_update_time     = pd.to_datetime('{:%Y-%m-%d} {}'.format(now, cboe_historical_update_time_str)).\
        tz_localize('America/Chicago').tz_convert('UTC')

# Time when CBOE updates daily settlement values.
cboe_daily_update_time_str = '15:30' # Chicago time
cboe_daily_update_time     = pd.to_datetime('{:%Y-%m-%d} {}'.format(now, cboe_daily_update_time_str)).\
        tz_localize('America/Chicago').tz_convert('UTC')

# References to CBOE's daily settlement futures data.
cboe_current_base_url = 'http://cfe.cboe.com/market-data'

# Other CBOE information.
num_active_vx_contracts = 8

def fetch_vx_contracts(period, force_update=False):
    """
    Retrieve VX contracts from CBOE that traded over a given timeframe.

    Parameters
    ----------
    period : pd.DatetimeIndex
        Target timeframe.

    force_update : bool
        Always update cache.

    Returns
    -------
    pd.DataFrame
        VX contracts concatenated together.

    Examples
    --------
    >>> period = pd.date_range(
            start = datetime(2016,1,15, tzinfo=pytz.timezone('UTC')),
            end   = datetime(2017,1,15, tzinfo=pytz.timezone('UTC')),
            freq  = cboe.bday_us)
    >>> vx_df = cboe.fetch_vx_contracts(period)
    >>> vx_df[['Futures', 'Expiration Date', 'Settle']]
                                  Futures            Expiration Date  Settle
    Trade Date
    2016-01-15 00:00:00+00:00  G (Feb 16)  2016-02-17 00:00:00+00:00  24.350
    2016-01-15 00:00:00+00:00  H (Mar 16)  2016-03-16 00:00:00+00:00  23.650
    2016-01-15 00:00:00+00:00  J (Apr 16)  2016-04-20 00:00:00+00:00  23.350
    2016-01-15 00:00:00+00:00  K (May 16)  2016-05-18 00:00:00+00:00  23.075
    2016-01-15 00:00:00+00:00  M (Jun 16)  2016-06-15 00:00:00+00:00  23.025
    2016-01-15 00:00:00+00:00  N (Jul 16)  2016-07-20 00:00:00+00:00  23.125
    2016-01-15 00:00:00+00:00  Q (Aug 16)  2016-08-17 00:00:00+00:00  22.975
    2016-01-15 00:00:00+00:00  U (Sep 16)  2016-09-21 00:00:00+00:00  23.200
    2016-01-19 00:00:00+00:00  G (Feb 16)  2016-02-17 00:00:00+00:00  24.025
    2016-01-19 00:00:00+00:00  H (Mar 16)  2016-03-16 00:00:00+00:00  23.400
    2016-01-19 00:00:00+00:00  J (Apr 16)  2016-04-20 00:00:00+00:00  23.125
    2016-01-19 00:00:00+00:00  K (May 16)  2016-05-18 00:00:00+00:00  22.950
    2016-01-19 00:00:00+00:00  M (Jun 16)  2016-06-15 00:00:00+00:00  22.825
    2016-01-19 00:00:00+00:00  N (Jul 16)  2016-07-20 00:00:00+00:00  22.950
    2016-01-19 00:00:00+00:00  Q (Aug 16)  2016-08-17 00:00:00+00:00  22.825
    2016-01-19 00:00:00+00:00  U (Sep 16)  2016-09-21 00:00:00+00:00  23.150
    2016-01-20 00:00:00+00:00  G (Feb 16)  2016-02-17 00:00:00+00:00  24.725
    2016-01-20 00:00:00+00:00  H (Mar 16)  2016-03-16 00:00:00+00:00  23.925
    2016-01-20 00:00:00+00:00  J (Apr 16)  2016-04-20 00:00:00+00:00  23.575
    2016-01-20 00:00:00+00:00  K (May 16)  2016-05-18 00:00:00+00:00  23.225
    2016-01-20 00:00:00+00:00  M (Jun 16)  2016-06-15 00:00:00+00:00  23.175
    2016-01-20 00:00:00+00:00  N (Jul 16)  2016-07-20 00:00:00+00:00  23.200
    2016-01-20 00:00:00+00:00  Q (Aug 16)  2016-08-17 00:00:00+00:00  23.125
    2016-01-20 00:00:00+00:00  U (Sep 16)  2016-09-21 00:00:00+00:00  23.300
    2016-01-21 00:00:00+00:00  G (Feb 16)  2016-02-17 00:00:00+00:00  25.075
    2016-01-21 00:00:00+00:00  H (Mar 16)  2016-03-16 00:00:00+00:00  24.075
    2016-01-21 00:00:00+00:00  J (Apr 16)  2016-04-20 00:00:00+00:00  23.675
    2016-01-21 00:00:00+00:00  K (May 16)  2016-05-18 00:00:00+00:00  23.325
    2016-01-21 00:00:00+00:00  M (Jun 16)  2016-06-15 00:00:00+00:00  23.225
    2016-01-21 00:00:00+00:00  N (Jul 16)  2016-07-20 00:00:00+00:00  23.300
    ...                               ...                        ...     ...
    2017-01-10 00:00:00+00:00  N (Jul 17)  2017-07-19 00:00:00+00:00  18.575
    2017-01-10 00:00:00+00:00  Q (Aug 17)  2017-08-16 00:00:00+00:00  18.750
    2017-01-10 00:00:00+00:00  U (Sep 17)  2017-09-20 00:00:00+00:00  19.175
    2017-01-11 00:00:00+00:00  F (Jan 17)  2017-01-18 00:00:00+00:00  12.525
    2017-01-11 00:00:00+00:00  G (Feb 17)  2017-02-15 00:00:00+00:00  14.225
    2017-01-11 00:00:00+00:00  H (Mar 17)  2017-03-22 00:00:00+00:00  15.625
    2017-01-11 00:00:00+00:00  J (Apr 17)  2017-04-19 00:00:00+00:00  16.725
    2017-01-11 00:00:00+00:00  K (May 17)  2017-05-17 00:00:00+00:00  17.325
    2017-01-11 00:00:00+00:00  M (Jun 17)  2017-06-21 00:00:00+00:00  17.825
    2017-01-11 00:00:00+00:00  N (Jul 17)  2017-07-19 00:00:00+00:00  18.425
    2017-01-11 00:00:00+00:00  Q (Aug 17)  2017-08-16 00:00:00+00:00  18.675
    2017-01-11 00:00:00+00:00  U (Sep 17)  2017-09-20 00:00:00+00:00  19.150
    2017-01-12 00:00:00+00:00  F (Jan 17)  2017-01-18 00:00:00+00:00  12.475
    2017-01-12 00:00:00+00:00  G (Feb 17)  2017-02-15 00:00:00+00:00  14.325
    2017-01-12 00:00:00+00:00  H (Mar 17)  2017-03-22 00:00:00+00:00  15.725
    2017-01-12 00:00:00+00:00  J (Apr 17)  2017-04-19 00:00:00+00:00  16.875
    2017-01-12 00:00:00+00:00  K (May 17)  2017-05-17 00:00:00+00:00  17.525
    2017-01-12 00:00:00+00:00  M (Jun 17)  2017-06-21 00:00:00+00:00  17.975
    2017-01-12 00:00:00+00:00  N (Jul 17)  2017-07-19 00:00:00+00:00  18.625
    2017-01-12 00:00:00+00:00  Q (Aug 17)  2017-08-16 00:00:00+00:00  18.825
    2017-01-12 00:00:00+00:00  U (Sep 17)  2017-09-20 00:00:00+00:00  19.325
    2017-01-13 00:00:00+00:00  F (Jan 17)  2017-01-18 00:00:00+00:00  12.175
    2017-01-13 00:00:00+00:00  G (Feb 17)  2017-02-15 00:00:00+00:00  14.225
    2017-01-13 00:00:00+00:00  H (Mar 17)  2017-03-22 00:00:00+00:00  15.725
    2017-01-13 00:00:00+00:00  J (Apr 17)  2017-04-19 00:00:00+00:00  16.975
    2017-01-13 00:00:00+00:00  K (May 17)  2017-05-17 00:00:00+00:00  17.650
    2017-01-13 00:00:00+00:00  M (Jun 17)  2017-06-21 00:00:00+00:00  18.125
    2017-01-13 00:00:00+00:00  N (Jul 17)  2017-07-19 00:00:00+00:00  18.775
    2017-01-13 00:00:00+00:00  Q (Aug 17)  2017-08-16 00:00:00+00:00  18.975
    2017-01-13 00:00:00+00:00  U (Sep 17)  2017-09-20 00:00:00+00:00  19.400

    [2212 rows x 3 columns]

    """
    # Resample period into months plus future months over which active contracts expire.
    months = pd.date_range(start=period[0], end=(period[-1]+num_active_vx_contracts*MonthBegin()), freq='MS')
    logger.debug('months =\n{}'.format(months))

    # Load VX contracts.
    vx_contracts = [fetch_vx_monthly_contract(d, force_update=force_update) for d in months]

    # Merge homogeneous dataframes (contracts) into a single dataframe, indexed by trading day.
    vx_contract_df = pd.concat(vx_contracts).set_index('Trade Date', drop=False)

    # Exclude invalid entries and entries outside the target timeframe.
    vx_contract_df = vx_contract_df.loc[period]
    vx_contract_df = vx_contract_df.dropna()

    # Get the most recent business day.
    post_date = (now - bday_us*(not is_business_day(now))).normalize()
    if(now < cboe_daily_update_time):
        post_date = post_date - bday_us
    logger.debug('post_date = {:%Y-%m-%d}'.format(post_date))

    if(post_date in period):
        # Append daily settlement values of the monthly VX contracts.
        vx_ds_df   = fetch_vx_daily_settlement()
        current_df = pd.DataFrame([{
            'Trade Date':      post_date,
            'Futures':         '{} ({} {})'.format(
                month_code[vx_ds_df.loc[i, 'Expiration Date'].month],
                calendar.month_abbr[vx_ds_df.loc[i, 'Expiration Date'].month],
                vx_ds_df.loc[i, 'Expiration Date'].year - cboe_base_millennium),
            'Expiration Date': vx_ds_df.loc[i, 'Expiration Date'],
            'Settle':          vx_ds_df.loc[i, 'SettlementPrice'],
            'Open':np.nan,'High':np.nan,'Low':np.nan,'Close':np.nan,
            'Change':np.nan,'Total Volume':np.nan,'EFP':np.nan,'Open Interest':np.nan}
            for i in vx_ds_df.index])
        current_df = current_df.set_index(current_df['Trade Date'], drop=False)
        vx_contract_df = vx_contract_df.append(current_df)
    return(vx_contract_df)
#END: fetch_vx_contracts

def fetch_vx_monthly_contract(monthyear, cache=True, force_update=False, cache_dir='.data'):
    """
    Retrieve historical data and expiration date from CBOE or local cache.

    Parameters
    ----------
    monthyear : datetime
        Contract's month and year of expiration.

    cache : bool
        Enable cache.

    force_update : bool
        Always update cache.

    cache_dir : str
        Cache's base directory path.

    Returns
    -------
    pd.DataFrame
        VX contract.
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

def fetch_vx_daily_settlement():
    """
    Read today's monthly VX settlement values from CBOE.

    Returns
    -------
    pd.DataFrame
        Daily settlement values of monthly VX contracts.

    Examples
    --------
    >>> ds = cboe.fetch_vx_daily_settlement()
    >>> ds
               Symbol  SettlementPrice           Expiration Date
    0   VX 04/19/2017           16.325 2017-04-19 00:00:00+00:00
    4   VX 05/17/2017           15.225 2017-05-17 00:00:00+00:00
    7   VX 06/21/2017           15.275 2017-06-21 00:00:00+00:00
    8   VX 07/19/2017           15.825 2017-07-19 00:00:00+00:00
    9   VX 08/16/2017           16.100 2017-08-16 00:00:00+00:00
    10  VX 09/20/2017           16.750 2017-09-20 00:00:00+00:00
    11  VX 10/18/2017           17.075 2017-10-18 00:00:00+00:00
    12  VX 11/15/2017           17.300 2017-11-15 00:00:00+00:00
    13  VX 12/20/2017           17.275 2017-12-20 00:00:00+00:00

    """
    # CBOE's Format:
    #    Symbol         SettlementPrice
    #    VX MM/DD/YYYY  *.***               <-- Front month
    #    VX** ExpDate2  *.***               <-- Weekly 1
    #    VX** ExpDate3  *.***               <-- Weekly 2
    #    VX** ExpDate4  *.***               <-- Weekly 3
    #    VX ExpDate5    *.***               <-- Back month
    #    ...
    try:
        vx_eod_values = pd.read_csv('{}/futures-settlements'.format(cboe_current_base_url),
                header=0, names=['Symbol', 'SettlementPrice'])
        logger.debug('Fetched data from CSV.')
    except: # fallback to HTML table
        try:
            cboe_tables = pd.read_html('{}/vx-futures-daily-settlement-prices'.format(cboe_current_base_url),
                    match='Settlement Price', header=0)
            cboe_tables[0].columns = [re.sub('\s+', ' ', x.strip()) for x in cboe_tables[0]]
            vx_eod_values = pd.DataFrame()
            vx_eod_values['Symbol']          = cboe_tables[0]['Symbol']
            vx_eod_values['SettlementPrice'] = cboe_tables[0]['Daily Settlement Price']
            logger.debug('Fetched data from HTML.')
        except:
            logger.exception('Failed to download daily settlement values from CBOE.')
            raise

    logger.debug('vx_eod_values =\n' + str(vx_eod_values))

    # Grab the front and back month expirations and settlement prices.
    p_monthly_expdate      = re.compile('VX \s*(.*)')
    monthly_vx_eod_values  = pd.DataFrame(
            vx_eod_values[
                vx_eod_values['Symbol'].apply(lambda x: p_monthly_expdate.match(x) is not None)
            ])
    try:
        front_month_eod_value = monthly_vx_eod_values.iloc[0]
        back_month_eod_value  = monthly_vx_eod_values.iloc[1]
    except:
        logger.exception('Failed to find monthly contract settlement data.')
        raise

    logger.debug('type(monthly_vx_eod_values) = {}'.format(type(monthly_vx_eod_values)))
    logger.debug('monthly_vx_eod_values =\n{}'.format(monthly_vx_eod_values))

    try:
        monthly_vx_eod_values['Expiration Date'] = [
                pd.to_datetime(
                    p_monthly_expdate.match(symbol).group(1),
                    format='%m/%d/%Y'
                    ).tz_localize('UTC')
                for symbol in monthly_vx_eod_values['Symbol']
                ]
        front_month_eod_value = monthly_vx_eod_values.iloc[0]
        back_month_eod_value  = monthly_vx_eod_values.iloc[1]
    except:
        logger.exception('Failed to read monthly contract expiration dates.')
        raise

    logger.debug('monthly_vx_eod_values =\n{}'.format(monthly_vx_eod_values))

    front_month_expdate = front_month_eod_value['Expiration Date']
    back_month_expdate  = back_month_eod_value['Expiration Date']
    front_month_price   = front_month_eod_value['SettlementPrice']
    back_month_price    = back_month_eod_value['SettlementPrice']

    logger.debug('front_month_expdate = {}'.format(front_month_expdate))
    logger.debug('back_month_expdate  = {}'.format(back_month_expdate ))
    logger.debug('front_month_price   = {}'.format(front_month_price  ))
    logger.debug('back_month_price    = {}'.format(back_month_price   ))
    return(monthly_vx_eod_values)
#END: fetch_vx_daily_settlement

def is_cboe_cache_current(contract, expdate, cache_path):
    """
    Test whether or not the contract's cache is up-to-date.

    Parameters
    ----------
    contract : pd.DataFrame
        Contract dataframe.

    expdate : datetime
        Contract's expiration date.

    cache_path : str
        File path to contract's cache.

    Returns
    -------
    bool
        Contract's cache is up-to-date.
    """
    is_current = True
    try:
        # Check if contract is expired or cache is up-to-date.
        current_datetime = cboe_historical_update_time if(now >= cboe_historical_update_time) else\
                (cboe_historical_update_time - bday_us)
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
    """
    Return the expiration date of a given VX contract.

    Parameters
    ----------
    monthyear : datetime
        Contract's month and year of expiration.

    Returns
    -------
    datetime
        Contract's expiration date.
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
    """
    Test if date is a business day.

    Parameters
    ----------
    date : datetime
        Date of interest.

    Returns
    -------
    bool
        Date is a business day.
    """
    return(len(pd.date_range(start=date, end=date, freq=bday_us)) > 0)
#END: is_business_day

def count_business_days(start, end):
    """
    Count the number of business days between two dates.

    Parameters
    ----------
    start : datetime
        First date.

    end : datetime
        Second date.

    Returns
    -------
    int
        Number of business days from start (inclusive) to end (exclusive).
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

def build_continuous_vx_dataframe(vx_contract_df):
    """
    Build dataframe of continuous VX data.

    Parameters
    ----------
    vx_contract_df : pd.DataFrame
        Dataframe containing VX contract data (see cboe.fetch_vx_contracts).

    Returns
    -------
    pd.DataFrame
        Continuous VX data.

    Examples
    --------
    Calculate continuous VX data from January 15 2017 to today (April 14 2017
    at the time this code was executed). Note that the continuous timeframe
    starts from the expiration date of the oldest contract within the given
    time period (period).
    >>> period = pd.date_range(
            start = datetime(2017,1,15, tzinfo=pytz.timezone('UTC')),
            end   = cboe.now,
            freq  = cboe.bday_us)
    >>> vx_contract_df = cboe.fetch_vx_contracts(period)
    >>> vx_continuous_df = cboe.build_continuous_vx_dataframe(vx_contract_df)
    >>> vx_continuous_df[['Front-Month Settle', 'Back-Month Settle', 'STCMVF']]
                               Front-Month Settle  Back-Month Settle     STCMVF
    Trade Date
    2017-02-15 00:00:00+00:00              12.875             14.025  12.922917
    2017-02-16 00:00:00+00:00              12.975             14.175  13.075000
    2017-02-17 00:00:00+00:00              13.125             14.350  13.278125
    2017-02-21 00:00:00+00:00              13.175             14.525  13.400000
    2017-02-22 00:00:00+00:00              13.225             14.625  13.516667
    2017-02-23 00:00:00+00:00              13.525             15.225  13.950000
    2017-02-24 00:00:00+00:00              13.325             15.250  13.886458
    2017-02-27 00:00:00+00:00              13.275             15.125  13.891667
    2017-02-28 00:00:00+00:00              13.525             15.325  14.200000
    2017-03-01 00:00:00+00:00              13.275             15.075  14.025000
    2017-03-02 00:00:00+00:00              13.275             15.025  14.077083
    2017-03-03 00:00:00+00:00              12.825             14.575  13.700000
    2017-03-06 00:00:00+00:00              12.675             14.425  13.622917
    2017-03-07 00:00:00+00:00              12.625             14.475  13.704167
    2017-03-08 00:00:00+00:00              12.675             14.425  13.768750
    2017-03-09 00:00:00+00:00              12.775             14.425  13.875000
    2017-03-10 00:00:00+00:00              12.525             14.200  13.711458
    2017-03-13 00:00:00+00:00              12.125             13.875  13.437500
    2017-03-14 00:00:00+00:00              12.625             14.075  13.772917
    2017-03-15 00:00:00+00:00              12.275             13.725  13.483333
    2017-03-16 00:00:00+00:00              11.925             13.275  13.106250
    2017-03-17 00:00:00+00:00              11.775             13.275  13.150000
    2017-03-20 00:00:00+00:00              11.625             13.175  13.110417
    2017-03-21 00:00:00+00:00              12.175             13.775  13.775000
    2017-03-22 00:00:00+00:00              13.925             14.575  13.959211
    2017-03-23 00:00:00+00:00              14.275             14.775  14.327632
    2017-03-24 00:00:00+00:00              13.925             14.400  14.000000
    2017-03-27 00:00:00+00:00              13.575             14.075  13.680263
    2017-03-28 00:00:00+00:00              12.925             13.550  13.089474
    2017-03-29 00:00:00+00:00              12.925             13.525  13.114474
    2017-03-30 00:00:00+00:00              12.825             13.375  13.027632
    2017-03-31 00:00:00+00:00              13.275             13.575  13.401316
    2017-04-03 00:00:00+00:00              13.475             13.575  13.522368
    2017-04-04 00:00:00+00:00              13.225             13.375  13.303947
    2017-04-05 00:00:00+00:00              13.875             13.875  13.875000
    2017-04-06 00:00:00+00:00              13.575             13.525  13.543421
    2017-04-07 00:00:00+00:00              14.025             13.875  13.922368
    2017-04-10 00:00:00+00:00              15.025             14.325  14.509211
    2017-04-11 00:00:00+00:00              15.975             14.525  14.830263
    2017-04-12 00:00:00+00:00              16.275             14.925  15.138158
    2017-04-13 00:00:00+00:00              16.325             15.225  15.340789

    """
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
    vx_continuous_df = pd.DataFrame(index=vx_pm_s.index) # short-term (front/back-month weighted)
    logger.debug('vx_pm_s.index =\n{}'.format(vx_pm_s.index))

    # Calculate short-term columns.
    vx_continuous_df['Prior-Month Expiration Date'] = vx_pm_s
    vx_continuous_df['Front-Month Settle']          = vx_fm_df['Settle']
    vx_continuous_df['Front-Month Expiration Date'] = vx_fm_df['Expiration Date']
    vx_continuous_df['Back-Month Settle']           = vx_bm_df['Settle']
    vx_continuous_df['Back-Month Expiration Date']  = vx_bm_df['Expiration Date']
    vx_continuous_df['Roll Period']                 = count_business_days(
            vx_continuous_df['Prior-Month Expiration Date'], vx_continuous_df['Front-Month Expiration Date'])
    vx_continuous_df['Days Till Rollover']          = (count_business_days(
            vx_continuous_df.index, vx_continuous_df['Front-Month Expiration Date']) - 1)
    vx_continuous_df['Front-Month Weight']          = vx_continuous_df['Days Till Rollover'] / vx_continuous_df['Roll Period']
    vx_continuous_df['Back-Month Weight']           = 1.0 - vx_continuous_df['Front-Month Weight']
    vx_continuous_df['STCMVF']                      =\
        vx_continuous_df['Front-Month Weight'] * vx_continuous_df['Front-Month Settle'] +\
        vx_continuous_df['Back-Month Weight'] * vx_continuous_df['Back-Month Settle']

    logger.debug('vx_continuous_df =\n{}'.format(vx_continuous_df[['Front-Month Expiration Date','Roll Period',
        'Days Till Rollover','Front-Month Weight','Back-Month Weight']]))
    return(vx_continuous_df)
#END: build_continuous_vx_dataframe

def test_plot():
    """Test unit that plots STCMVF and VIX over time."""

    # Debug-level logging.
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('cboe.test_plot.log', 'w')
    fh.setFormatter(fmt)
    con = logging.StreamHandler(sys.stdout)
    con.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(con)

    # Setup timeframe to cover last 2 years from the most recent business day.
    timeframe     = 2*200
    end_date      = (now - bday_us*(not is_business_day(now))).normalize()
    start_date    = end_date - timeframe*bday_us
    target_period = pd.date_range(start=start_date, end=end_date, freq=bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # Load VX contracts.
    vx_contract_df = fetch_vx_contracts(target_period)

    # Build dataframe of continuous VX data.
    vx_continuous_df = build_continuous_vx_dataframe(vx_contract_df)

    # Fetch VIX daily quotes from Yahoo! Finance.
    vix_df = web.DataReader('^VIX', 'yahoo',
            start=vx_continuous_df.index[0], end=vx_continuous_df.index[-1])
    vix_df = vix_df.tz_localize('UTC') # make dates timezone-aware
    vx_continuous_df['VIX'] = vix_df['Adj Close']

    # Plot
    vx_continuous_df[['VIX','STCMVF']].plot()
    plt.savefig('st.png')

    # Drop into a Python shell with all definitions.
    code.interact(local=dict(globals(), **locals()))

    # Test done. Reset logging.
    logger.setLevel(logging.WARNING)
#END: test_plot

if(__name__ == '__main__'):
    test_plot()
