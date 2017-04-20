#!/usr/bin/python
#
# Calculate the daily short-term VIX futures value and
# post value to StockTwits.

from credentials import st_access_token
import cboe
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np
import requests
import ssl
import mimetypes
import sys
import logging
import logging.config

# Log setup
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('post_STCMVF')

def main():
    # Is today a business day? If not, quit.
    if(not cboe.is_business_day(cboe.now)):
        logger.debug('Today ({:%Y-%m-%d}) is a non-workday. Aborting...'.format(cboe.now))
        sys.exit()
    logger.debug('Today ({:%Y-%m-%d}) is a workday. Proceeding...'.format(cboe.now))

    # Setup timeframe to cover last 2 years from the most recent business day.
    years         = 2
    end_date      = (cboe.now - cboe.bday_us*(not cboe.is_business_day(cboe.now))).normalize()
    start_date    = end_date - years*365*cboe.Day()
    target_period = pd.date_range(start=start_date, end=end_date, freq=cboe.bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # StockTwits settings.
    st_dry_run        = False
    st_post_cash_tags = True
    st_cash_tags      = '$VXX $XIV $SVXY $TVIX $UVXY'
    st_post_chart     = True
    chart_file        = 'chart.png'
    st_preamble       = st_cash_tags if st_post_cash_tags else '(TEST)'
    st_message        = '{} Short-term constant-maturity VIX futures (STCMVF) settled @ {:.3f} ({:+.1%}).'

    # Load VX contracts.
    vx_contract_df = cboe.fetch_vx_contracts(target_period)
    logger.debug('vx_contract_df =\n{}'.format(vx_contract_df))

    # Build dataframe of continuous VX data.
    vx_continuous_df = cboe.build_continuous_vx_dataframe(vx_contract_df)
    logger.debug('vx_continuous_df =\n{}'.format(vx_continuous_df))

    # Add 'VIX' column to continuous dataframe.
    (vix_df, success) = fetch_yahoo_ticker('^VIX', vx_continuous_df.index)
    if(not success):
        st_post_chart = False
    else:
        vx_continuous_df['VIX'] = vix_df['Adj Close']

    if(st_post_chart):
        # Plot to image file.
        generate_vx_figure(vx_continuous_df)
        plt.savefig(chart_file, dpi=300)

    # Get recent VX quotes.
    vx_yesterday     = vx_continuous_df.iloc[-2]
    vx_today         = vx_continuous_df.iloc[-1]
    stcmvf_yesterday = vx_yesterday['STCMVF']
    stcmvf_today     = vx_today['STCMVF']
    stcmvf_percent   = (stcmvf_today / stcmvf_yesterday) - 1.0
    logger.debug('vx_yesterday =\n{}'.format(vx_yesterday))
    logger.debug('vx_today =\n{}'.format(vx_today))

    # Post to StockTwits.
    st_message = st_message.format(st_preamble, stcmvf_today, stcmvf_percent)
    logger.debug('st_message = {}'.format(st_message))

    if(st_post_chart):
        st_attachment = chart_file
        logger.debug('Posting message with {}.'.format(chart_file))
    else:
        st_attachment = None
    post_to_stocktwits(st_access_token, st_message, attachment=st_attachment,
            dry_run=st_dry_run)
#END: main

def fetch_yahoo_ticker(ticker, index):
    """
    Retrieve data for ticker over a given period from Yahoo! Finance. Note
    that Yahoo! Finance does NOT include today's data.

    Parameters
    ----------
    ticker : str
        Stock/ETF/Index ticker.

    index : pd.DatetimeIndex
        Period over which to fetch the data.

    Returns
    -------
    (pd.DataFrame, bool)
        Ticker data from Yahoo! Finance and whether or not successful. Columns provided are:
            Open
            High
            Low
            Close
            Volume
            Adj Close
    """
    try:
        # Fetch VIX daily quotes from Yahoo! Finance.
        vix_df = web.DataReader('^VIX', 'yahoo', start=index[0], end=index[-1])
        vix_df = vix_df.tz_localize('UTC') # make dates timezone-aware
        logger.debug('vix_df =\n{}'.format(vix_df))
        success = True
    except:
        logger.warning('Failed to download VIX index values from Yahoo! Finance.')
        success = False
    return(vix_df, success)
#END: fetch_yahoo_ticker

def generate_vx_figure(vx_continuous_df):
    """
    Create the continuous VX figure, which plots STCMVF and VIX over time, the
    percent difference between STCMVF and VIX, and a histogram of STCMVF.

    Parameters
    ----------
    vx_continuous_df : pd.DataFrame
        Dataframe generated from cboe.build_continuous_vx_dataframe with an
        added column, 'VIX', that represents VIX's values.
    """
    years = np.ceil((vx_continuous_df.index[-1] - vx_continuous_df.index[0]).days / 365.0)

    # Setup a grid of sub-plots.
    fig = plt.figure(1)
    gs  = gridspec.GridSpec(2, 2, height_ratios=[2, 1], width_ratios=[2, 1])

    # VIX vs STCMVF
    timeseries_axes1 = plt.subplot(gs[0])
    timeseries_axes1.plot(vx_continuous_df['VIX'], label='VIX')
    timeseries_axes1.plot(vx_continuous_df['STCMVF'], label='STCMVF', alpha=0.75)
    plt.setp(timeseries_axes1.get_xticklabels(), visible=False) # hide date labels on top subplot
    plt.grid(True)
    plt.ylabel('Volatility Level')
    plt.title('{:0.0f}-Year Daily Chart'.format(years))

    # Percent difference between STCMVF and VIX
    timeseries_axes2 = plt.subplot(gs[2], sharex=timeseries_axes1)
    timeseries_axes2.plot(((vx_continuous_df['STCMVF'] / vx_continuous_df['VIX']) - 1.0) * 100.0,
            'k-')
    plt.grid(True)
    ys, ye = timeseries_axes2.get_ylim()
    ystep  = 10.0
    logger.debug('ys, ye (before)= {}, {}'.format(ys, ye))
    ys = np.sign(ys)*np.floor(np.abs(ys)/ystep)*ystep # round-down to nearest ystep
    ye = np.sign(ye)*np.ceil(np.abs(ye)/ystep)*ystep # round-up to nearest ystep
    logger.debug('ys, ye (after)= {}, {}'.format(ys, ye))
    plt.yticks(np.arange(ys, ye, ystep)) # set rounded y-values stepped by ystep
    plt.ylabel('STCMVF-VIX (%)')

    # Histogram of STCMVF
    hist_axes = plt.subplot(gs[1])
    hist_axes.hist(vx_continuous_df['VIX'].dropna(), bins='auto', label='VIX')
    hist_axes.hist(vx_continuous_df['STCMVF'].dropna(), bins='auto', label='STCMVF', alpha=0.75)
    plt.grid(True)
    xs, xe = hist_axes.get_xlim()
    xstep  = 5.0
    logger.debug('xs, xe (before)= {}, {}'.format(xs, xe))
    xs = np.max([10.0, np.sign(xs)*np.floor(np.abs(xs)/xstep)*xstep]) # round-down to nearest xstep
    xe = np.sign(xe)*np.ceil(np.abs(xe)/xstep)*xstep # round-up to nearest xstep
    logger.debug('xs, xe (after)= {}, {}'.format(xs, xe))
    plt.xticks(np.arange(xs, xe, xstep)) # set rounded x-values stepped by 5
    plt.xlabel('Volatility Level')
    plt.ylabel('Occurrences')
    plt.title('Histogram')
    plt.legend(bbox_to_anchor=(0.5, -0.25), loc='upper center', ncol=1) # place legend below histogram

    # Minor adjustments
    plt.setp(timeseries_axes2.get_xticklabels(), rotation=60, # rotate dates along x-axis
            horizontalalignment='right')
    plt.subplots_adjust(bottom=0.2, hspace=0.1, wspace=0.3) # adjust spacing between and around sub-plots
#END: generate_vx_figure

def post_to_stocktwits(access_token, message, attachment=None, dry_run=False):
    """
    Post message and attachment (optional) to StockTwits using the given access token
    (see https://stocktwits.com/developers/docs/authentication). Messages must be
    less than 140 characters.

    Parameters
    ----------
    access_token : str
        Token generated from StockTwits used for authentication.

    message : str
        Test message to be posted.

    attachment : str
        Path to image to be attached with message. File formats accepted: JPG, PNG, and
        GIF under 2MB. Counts as 24 characters if specified.

    dry_run : bool
        Do not actually post message.
    """
    total_count = len(message)
    payload = {'access_token':access_token, 'body':message}
    if(attachment):
        (attachment_type, encoding) = mimetypes.MimeTypes().guess_type(attachment)
        files = {'chart':(attachment, open(attachment, 'rb'), attachment_type)}
        total_count += 24
    else:
        files = None
    logger.debug('total_count = ' + str(total_count))
    logger.debug('payload = ' + str(payload))

    if(total_count > 140):
        log.error('Message length, {}, exceeds 140 characters.'.format(total_count))

    try:
        if(dry_run):
            logger.debug('Dry-run is enabled so will not post.')
            return
        # Post message.
        r = requests.post('https://api.stocktwits.com/api/2/messages/create.json', data=payload,
                files=files)
        # Check StockTwit's response.
        r = r.json()
        logger.debug('Response from StockTwits = ' + str(r))
        status = r['response']['status']
        logger.debug('Status from StockTwits = ' + str(status))
        if(status != 200):
            raise Exception('Received invalid response from StockTwits: ' + str(status) + ': ' + str(r))
    except:
        logger.exception('Failed to post to StockTwits.')
        raise

    logger.info('Posted message: ' + message)
#END: post_to_stocktwits

if(__name__ == '__main__'):
    main()
