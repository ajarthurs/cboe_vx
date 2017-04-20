#!/usr/bin/python
#
# Calculate the daily short-term VIX futures value and
# post value to StockTwits.

from credentials import st_access_token
import cboe
import pandas as pd
import pandas_datareader.data as web
import matplotlib.pyplot as plt
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
    timeframe     = 2*200
    end_date      = (cboe.now - cboe.bday_us*(not cboe.is_business_day(cboe.now))).normalize()
    start_date    = end_date - timeframe*cboe.bday_us
    target_period = pd.date_range(start=start_date, end=end_date, freq=cboe.bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # Load VX contracts.
    vx_contract_df = cboe.fetch_vx_contracts(target_period)
    logger.debug('vx_contract_df =\n{}'.format(vx_contract_df))

    # Build dataframe of continuous VX data.
    vx_continuous_df = cboe.build_continuous_vx_dataframe(vx_contract_df)
    logger.debug('vx_continuous_df =\n{}'.format(vx_continuous_df))

    # Fetch VIX daily quotes from Yahoo! Finance.
    vix_df = web.DataReader('^VIX', 'yahoo',
            start=vx_continuous_df.index[0], end=vx_continuous_df.index[-1])
    vix_df = vix_df.tz_localize('UTC') # make dates timezone-aware
    vx_continuous_df['VIX'] = vix_df['Adj Close']
    logger.debug('vix_df =\n{}'.format(vix_df))

    # Plot to stcmvf.png
    p = vx_continuous_df[['VIX','STCMVF']].plot()
    plt.savefig('stcmvf.png')
    logger.debug('plot = {}'.format(p))

    # Get recent VX quotes.
    vx_yesterday     = vx_continuous_df.iloc[-2]
    vx_today         = vx_continuous_df.iloc[-1]
    stcmvf_yesterday = vx_yesterday['STCMVF']
    stcmvf_today     = vx_today['STCMVF']
    stcmvf_percent   = (stcmvf_today / stcmvf_yesterday) - 1.0
    logger.debug('vx_yesterday =\n{}'.format(vx_yesterday))
    logger.debug('vx_today =\n{}'.format(vx_today))

    # Post to StockTwits.
    #st_message = '$VXX $XIV $SVXY $TVIX $UVXY Short-term constant-maturity VIX futures settled @ ' +\
    st_message = 'Short-term constant-maturity VIX futures settled @ ' +\
            '{:.3f} ({:+.1%}).'.format(stcmvf_today, stcmvf_percent)
    logger.debug('st_message = {}'.format(st_message))
    #post_to_stocktwits('my-StockTwits-access-token', st_message, attachment='stcmvf.png', dry_run=True)
    post_to_stocktwits(st_access_token, st_message,
            attachment='stcmvf.png',
            #dry_run=False)
            dry_run=True)
#END: main

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
