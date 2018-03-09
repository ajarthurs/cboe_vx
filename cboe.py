#!/usr/bin/python

"""Read and process futures data from CBOE."""

import numpy as np
import pandas as pd
from holiday import USMarketHolidayCalendar
from pandas.tseries.offsets import CDay,Day,Week,MonthBegin,MonthEnd
from pandas import ExcelWriter
from pandas import plotting
plotting.register_matplotlib_converters()
import pytz
import calendar
import re
import pickle
import sys
import os
import logging

logger = logging.getLogger(__name__)

# References to the US Federal Government Holiday Calendar and current time.
calendar_us = USMarketHolidayCalendar()
bday_us     = CDay(calendar=calendar_us)
now         = pd.to_datetime('now', utc=True)
today       = now.tz_convert('America/Chicago').normalize()

# References to CBOE's historical futures data.
cboe_historical_index_base_url = 'https://cfe.cboe.com/Publish/ScheduledTask/MktData/datahouse'
cboe_historical_base_url = 'https://markets.cboe.com/us/futures/market_statistics/historical_data/products/csv'
cboe_base_millennium     = 2000
cboe_vx_adj_date         = pd.datetime(2007, 3, 23, tzinfo=pytz.timezone('UTC'))
#                  J    F    M    A    M    J    J    A    S    O    N    D
#                  a    e    a    p    a    u    u    u    e    c    o    e
#                  n    b    r    r    y    n    l    g    p    t    v    c
month_code = ['', 'F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
#             0    1    2    3    4    5    6    7    8    9   10   11   12
cboe_index = {'VIX' : 'vixcurrent.csv', 'VXMT' : 'vxmtdailyprices.csv'}

# Time when CBOE updates historical futures data.
cboe_historical_update_time_str = '10:00' # Chicago time
cboe_historical_update_time     = pd.to_datetime('{:%Y-%m-%d} {}'.format(now, cboe_historical_update_time_str)).\
        tz_localize('America/Chicago').tz_convert('UTC')

# Time when CBOE updates daily settlement values.
cboe_daily_update_time_str = '15:20' # Chicago time
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
    ...     start = datetime(2016,1,15, tzinfo=pytz.timezone('UTC')),
    ...     end   = datetime(2017,1,15, tzinfo=pytz.timezone('UTC')),
    ...     freq  = cboe.bday_us)
    >>> vx_df = cboe.fetch_vx_contracts(period)
    >>> vx_df[['Futures', 'Expiration Date', 'Settle']]
                                  Futures            Expiration Date  Settle
    Trade Date
    2016-01-15 00:00:00+00:00  F (Jan 16)  2016-01-20 00:00:00+00:00  26.475
    2016-01-15 00:00:00+00:00  G (Feb 16)  2016-02-17 00:00:00+00:00  24.350
    2016-01-15 00:00:00+00:00  H (Mar 16)  2016-03-16 00:00:00+00:00  23.650
    2016-01-15 00:00:00+00:00  J (Apr 16)  2016-04-20 00:00:00+00:00  23.350
    2016-01-15 00:00:00+00:00  K (May 16)  2016-05-18 00:00:00+00:00  23.075
    2016-01-15 00:00:00+00:00  M (Jun 16)  2016-06-15 00:00:00+00:00  23.025
    2016-01-15 00:00:00+00:00  N (Jul 16)  2016-07-20 00:00:00+00:00  23.125
    2016-01-15 00:00:00+00:00  Q (Aug 16)  2016-08-17 00:00:00+00:00  22.975
    2016-01-15 00:00:00+00:00  U (Sep 16)  2016-09-21 00:00:00+00:00  23.200
    2016-01-19 00:00:00+00:00  F (Jan 16)  2016-01-20 00:00:00+00:00  25.850
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
    <BLANKLINE>
    [2214 rows x 3 columns]

    """
    # Resample period into months plus future months over which active contracts expire.
    months = pd.date_range(start=(period[0]-MonthBegin()),
            end=(period[-1]+num_active_vx_contracts*MonthBegin()), freq='MS')
    logger.debug('months =\n{}'.format(months))

    # Load VX contracts.
    vx_contracts = [fetch_vx_monthly_contract(d, force_update=force_update) for d in months]

    # Merge homogeneous dataframes (contracts) into a single dataframe, indexed by trading day.
    vx_contract_df = pd.concat(vx_contracts).set_index('Trade Date', drop=False)
    logger.debug('vx_contract_df (unfiltered)=\n{}'.format(vx_contract_df))

    # Exclude invalid entries and entries outside the target timeframe.
    vx_contract_df = vx_contract_df.loc[period]
    vx_contract_df = vx_contract_df.dropna()

    return(vx_contract_df)
#END: fetch_vx_contracts

def fetch_vx_monthly_contract(monthyear, cache=True, force_update=False, cache_dir='.data'):
    """
    Retrieve historical data and expiration date from CBOE or local cache.

    Parameters
    ----------
    monthyear : datetime
        Contract's month and year of expiration. It is strongly recommended
        that the given day is the 1st of the month (see pandas.tseries.offsets.MonthBegin()).

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
            vx_contract = pd.read_csv(
                '{}/VX/{:%Y-%m-%d}'.format(cboe_historical_base_url, vx_expdate),
                header=1,
                names=['Trade Date','Futures','Open','High','Low','Close','Settle',
                    'Change','Total Volume','EFP','Open Interest'])
        except:
            logger.exception('Failed to download VX contract {} from CBOE.'.format(contract_name))
            raise

        try:
            # Parse dates (assuming MM/DD/YYYY format), set timezone to UTC, and reset to midnight.
            vx_contract['Trade Date'] = pd.to_datetime(vx_contract['Trade Date'],
                    format='%Y-%m-%d').apply(lambda x: x.tz_localize('UTC'))
            vx_contract['Trade Date'] = pd.DatetimeIndex(vx_contract['Trade Date']).normalize()
        except:
            logger.exception('Unexpected datetime format from CBOE.')
            raise

        # Discard entries at expiration and beyond.
        vx_contract = vx_contract[vx_contract['Trade Date'] < vx_expdate]

        # Adjust prices prior to March 23 2007 (divide by 10).
        vx_contract.ix[vx_contract['Trade Date'] < cboe_vx_adj_date, 'Settle'] /= 10.0
        vx_contract.ix[vx_contract['Trade Date'] < cboe_vx_adj_date, 'High']   /= 10.0
        vx_contract.ix[vx_contract['Trade Date'] < cboe_vx_adj_date, 'Low']    /= 10.0
        vx_contract.ix[vx_contract['Trade Date'] < cboe_vx_adj_date, 'Open']   /= 10.0
        vx_contract.ix[vx_contract['Trade Date'] < cboe_vx_adj_date, 'Close']  /= 10.0

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
        month4_eod_value      = monthly_vx_eod_values.iloc[3]
        month5_eod_value      = monthly_vx_eod_values.iloc[4]
        month6_eod_value      = monthly_vx_eod_values.iloc[5]
        month7_eod_value      = monthly_vx_eod_values.iloc[6]
    except:
        logger.exception('Failed to find monthly contract settlement data.')
        raise

    logger.debug('type(monthly_vx_eod_values) = {}'.format(type(monthly_vx_eod_values)))
    logger.debug('monthly_vx_eod_values =\n{}'.format(monthly_vx_eod_values))

    # Add datetime-formatted column of expiration dates.
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

    logger.debug('monthly_vx_eod_values(unfiltered) =\n{}'.format(monthly_vx_eod_values))

    # Filter out expired contracts.
    current_datetime = cboe_historical_update_time if(now >= cboe_historical_update_time) else\
            (cboe_historical_update_time - bday_us)
    monthly_vx_eod_values = monthly_vx_eod_values[monthly_vx_eod_values['Expiration Date'] > current_datetime]

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
#END: is_cboe_cache_current

def get_vx_expiration_date(monthyear):
    """
    Return the expiration date of a given VX contract.

    Parameters
    ----------
    monthyear : datetime
        Contract's month and year of expiration. It is strongly recommended
        that the given day is the 1st of the month (see pandas.tseries.offsets.MonthBegin()).

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
    Calculate continuous VX data from January 15 2017 to April 14 2017.
    >>> period = pd.date_range(
    ...     start = datetime(2017,1,15, tzinfo=pytz.timezone('UTC')),
    ...     end   = datetime(2017,4,14, tzinfo=pytz.timezone('UTC')),
    ...     freq  = cboe.bday_us)
    >>> vx_contract_df = cboe.fetch_vx_contracts(period)
    >>> vx_continuous_df = cboe.build_continuous_vx_dataframe(vx_contract_df)
    >>> vx_continuous_df[['Month1 Settle', 'Month2 Settle', 'STCMVF']]
                                    Month1 Settle      Month2 Settle     STCMVF
    Trade Date
    2017-01-17 00:00:00+00:00              12.175             14.175  14.175000
    2017-01-18 00:00:00+00:00              14.175             15.625  14.247500
    2017-01-19 00:00:00+00:00              14.225             15.575  14.360000
    2017-01-20 00:00:00+00:00              13.825             15.225  14.035000
    2017-01-23 00:00:00+00:00              13.575             14.975  13.855000
    2017-01-24 00:00:00+00:00              13.025             14.375  13.362500
    2017-01-25 00:00:00+00:00              12.725             14.175  13.160000
    2017-01-26 00:00:00+00:00              12.675             14.125  13.182500
    2017-01-27 00:00:00+00:00              12.525             14.125  13.165000
    2017-01-30 00:00:00+00:00              12.875             14.325  13.527500
    2017-01-31 00:00:00+00:00              12.925             14.325  13.625000
    2017-02-01 00:00:00+00:00              12.675             13.975  13.390000
    2017-02-02 00:00:00+00:00              12.925             14.175  13.675000
    2017-02-03 00:00:00+00:00              12.475             13.925  13.417500
    2017-02-06 00:00:00+00:00              12.475             13.975  13.525000
    2017-02-07 00:00:00+00:00              12.575             13.975  13.625000
    2017-02-08 00:00:00+00:00              12.575             13.975  13.695000
    2017-02-09 00:00:00+00:00              12.025             13.675  13.427500
    2017-02-10 00:00:00+00:00              11.725             13.375  13.210000
    2017-02-13 00:00:00+00:00              11.425             13.075  12.992500
    2017-02-14 00:00:00+00:00              11.175             12.300  12.300000
    2017-02-15 00:00:00+00:00              12.875             14.025  12.922917
    2017-02-16 00:00:00+00:00              12.975             14.175  13.075000
    2017-02-17 00:00:00+00:00              13.125             14.350  13.278125
    2017-02-21 00:00:00+00:00              13.175             14.525  13.400000
    2017-02-22 00:00:00+00:00              13.225             14.625  13.516667
    2017-02-23 00:00:00+00:00              13.525             15.225  13.950000
    2017-02-24 00:00:00+00:00              13.325             15.250  13.886458
    2017-02-27 00:00:00+00:00              13.275             15.125  13.891667
    2017-02-28 00:00:00+00:00              13.525             15.325  14.200000
    ...                                       ...                ...        ...
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
    <BLANKLINE>
    [62 rows x 3 columns]

    """
    timeframe      = vx_contract_df.index.unique()

    # Create GroupBy objects.
    vx_td_gb = vx_contract_df.groupby([pd.Grouper('Trade Date')])      # group by trading day
    vx_ed_gb = vx_contract_df.groupby('Expiration Date') # group by expiration day

    logger.debug('vx_td_gb =\n{}'.format(vx_td_gb))

    # Get list (series) of expiration dates
    vx_expdate_s    = pd.Series(vx_ed_gb.first().index) # build from given contract dataframe
    if(timeframe[0] < vx_expdate_s[0]):
        # Prepend list of expiration dates with prior expiration date within the given timeframe.
        prior_monthyear = vx_expdate_s[0] - MonthEnd() - MonthBegin()
        prior_expdate_s = pd.Series(get_vx_expiration_date(prior_monthyear))
        vx_expdate_s    = pd.concat([prior_expdate_s, vx_expdate_s])

    # Create continuous prior-month (m0) expiration date series, indexed by trading day.
    vx_pm_s = pd.Series(
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
    vx_m1_df = vx_td_gb.nth(0) # front-month
    vx_m2_df = vx_td_gb.nth(1) # back-month
    vx_m4_df = vx_td_gb.nth(3) # m4
    vx_m5_df = vx_td_gb.nth(4) # m5
    vx_m6_df = vx_td_gb.nth(5) # m6
    vx_m7_df = vx_td_gb.nth(6) # m7

    logger.debug('vx_fm_df =\n{}'.format(vx_m1_df))

    # Create custom dataframes indexed by trading day.
    vx_continuous_df = pd.DataFrame(index=vx_pm_s.index)
    logger.debug('vx_pm_s.index =\n{}'.format(vx_pm_s.index))

    # Calculate short-term columns.
    vx_continuous_df['Month0 Expiration Date'] = vx_pm_s
    vx_continuous_df['Month1 Settle']          = vx_m1_df['Settle']
    vx_continuous_df['Month1 Expiration Date'] = vx_m1_df['Expiration Date']
    vx_continuous_df['Month2 Settle']          = vx_m2_df['Settle']
    vx_continuous_df['Month4 Settle']          = vx_m4_df['Settle']
    vx_continuous_df['Month5 Settle']          = vx_m5_df['Settle']
    vx_continuous_df['Month6 Settle']          = vx_m6_df['Settle']
    vx_continuous_df['Month7 Settle']          = vx_m7_df['Settle']
    vx_continuous_df['Roll Period']            = count_business_days(
            vx_continuous_df['Month0 Expiration Date'], vx_continuous_df['Month1 Expiration Date'])
    vx_continuous_df['Days Till Rollover']     = (count_business_days(
            vx_continuous_df.index, vx_continuous_df['Month1 Expiration Date']) - 1)
    vx_continuous_df['ST Month1 Weight']       = vx_continuous_df['Days Till Rollover'] / vx_continuous_df['Roll Period']
    vx_continuous_df['ST Month2 Weight']       = 1.0 - vx_continuous_df['ST Month1 Weight']
    vx_continuous_df['MT Month4 Weight']       = (vx_continuous_df['Days Till Rollover'] / vx_continuous_df['Roll Period']) / 3.0
    vx_continuous_df['MT Month7 Weight']       = (1.0 / 3.0) - vx_continuous_df['MT Month4 Weight']
    vx_continuous_df['STCMVF']                 =\
        vx_continuous_df['ST Month1 Weight'] * vx_continuous_df['Month1 Settle'] +\
        vx_continuous_df['ST Month2 Weight'] * vx_continuous_df['Month2 Settle']
    vx_continuous_df['MTCMVF']                 =\
        vx_continuous_df['MT Month4 Weight'] * vx_continuous_df['Month4 Settle'] +\
        (1.0 / 3.0) * vx_continuous_df['Month5 Settle'] +\
        (1.0 / 3.0) * vx_continuous_df['Month6 Settle'] +\
        vx_continuous_df['MT Month7 Weight'] * vx_continuous_df['Month7 Settle']

    logger.debug('vx_continuous_df =\n{}'.format(vx_continuous_df[['Month1 Expiration Date','Roll Period',
        'Days Till Rollover','ST Month1 Weight']]))
    return(vx_continuous_df)
#END: build_continuous_vx_dataframe

def fetch_index(index):
    """
    Retrieve data of a CBOE index.

    Parameters
    ----------
    index : str
        Name of the CBOE index to fetch. Must be one of the following:
        'VXMT'

    Returns
    -------
    pd.DataFrame
        Index data.
    """
    try:
        import urllib
        from bs4 import BeautifulSoup

        index_df = pd.read_csv('{}/{}'.format(cboe_historical_index_base_url, cboe_index[index]),
                skiprows=2, header=1, names=['Date', 'Open', 'High', 'Low', 'Close'])
        logger.debug('index_df = \n{}'.format(index_df))
        stoday = '{:%m/%d/%Y}'.format(today)
        if(stoday not in index_df['Date'].values):
            # Fetch today's data from Google Finance
            google_page = urllib.request.urlopen('https://finance.google.com/finance?q=INDEXCBOE%3A{}'.format(index))
            google_soup = BeautifulSoup(google_page, 'html5lib')
            last_entry = pd.DataFrame([
                dict(
                    Date=stoday, Open=np.nan, High=np.nan, Low=np.nan,
                    Close=float(google_soup.find('span', class_='pr').span.string)
                    )
                ])
            index_df = index_df.append(last_entry)
            logger.debug('Appending to dataframe:\n{}'.format(last_entry))
        # Parse dates (assuming MM/DD/YYYY format), set timezone to UTC, and reset to midnight.
        index_df['Date'] = pd.to_datetime(index_df['Date'],
                format='%m/%d/%Y').apply(lambda x: x.tz_localize('UTC'))
        index_df['Date'] = pd.DatetimeIndex(index_df['Date']).normalize()
        index_df = index_df.set_index(index_df['Date'], drop=False)
        logger.debug('Fetched {} data:\n{}'.format(index, index_df))
    except:
        logger.exception('Failed to download {} data.'.format(index))
        raise
    return(index_df)
#END: fetch_index

def test_plot():
    """Test unit that plots STCMVF and VIX over time."""
    import pandas_datareader.data as web
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import code

    # Debug-level logging.
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s - %(name)s:%(funcName)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('cboe.test_plot.log', 'w')
    fh.setFormatter(fmt)
    con = logging.StreamHandler(sys.stdout)
    con.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(con)

    # Setup timeframe to cover from 1/1/2006 to the most recent business day.
    end_date      = (now - bday_us*(not is_business_day(today))).normalize()
    start_date    = pd.datetime(2006, 1, 1, tzinfo=pytz.timezone('UTC'))
    target_period = pd.date_range(start=start_date, end=end_date, freq=bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # Load VX contracts.
    vx_contract_df = fetch_vx_contracts(target_period)

    # Build dataframe of continuous VX data.
    vx_continuous_df = build_continuous_vx_dataframe(vx_contract_df)

    # Fetch VIX daily quotes from CBOE
    vix_df = fetch_index('VIX')
    vx_continuous_df['VIX'] = vix_df['Close']

    # Fetch VXMT daily quotes from CBOE
    vix_df = fetch_index('VXMT')
    vx_continuous_df['VXMT'] = vix_df['Close']

    # Plot
    vx_continuous_df[['VIX','STCMVF']].plot()
    plt.savefig('chart_test.png')

    # Write to Excel
    writer = ExcelWriter('vf_test.xlsx')
    vx_continuous_df.to_excel(writer)
    writer.save()

    # Drop into a Python shell with all definitions.
    code.interact(local=dict(globals(), **locals()))

    # Test done. Reset logging.
    logger.setLevel(logging.WARNING)
#END: test_plot

if(__name__ == '__main__'):
    test_plot()
