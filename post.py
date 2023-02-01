#!/usr/bin/python

"""Calculate, chart, and post STCMVF and MTCMVF to StockTwits."""

import settings
import cboe
import pandas as pd
from pandas import ExcelWriter
from pandas import plotting
plotting.register_matplotlib_converters()
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib import gridspec
import numpy as np
import requests
import ssl
import mimetypes
import pickle
import sys
import time
import pytz
import logging
import logging.config

# Log setup
logging.config.fileConfig('logging.conf')
logger = logging.getLogger('post')

def main():
    # Is today a business day? If not, quit.
    if(settings.check_for_holiday and not cboe.is_business_day(cboe.today)):
        logger.debug('Today ({:%Y-%m-%d}) is a non-workday. Aborting...'.format(cboe.today))
        sys.exit()
    logger.debug('Today ({:%Y-%m-%d}) is a workday. Proceeding...'.format(cboe.today))

    # Setup timeframe to cover last several years from the most recent business day.
    years         = max(settings.st_years, settings.mt_years)
    end_date      = cboe.last_settled_date
    start_date    = end_date - years*365*cboe.Day()
    target_period = pd.date_range(start=start_date, end=end_date, freq=cboe.bday_us)

    logger.debug('target_period =\n{}'.format(target_period))

    # Load VX contracts.
    vx_contract_df = cboe.fetch_vx_contracts(target_period)
    logger.debug('vx_contract_df =\n{}'.format(vx_contract_df))

    # Build dataframe of continuous VX data.
    vx_continuous_df = cboe.build_continuous_vx_dataframe(vx_contract_df)
    logger.debug('vx_continuous_df =\n{}'.format(vx_continuous_df))

    # Add 'VIX' column to continuous dataframe.
    try:
        vix_df = cboe.fetch_index('VIX')
        success = True
    except:
        success = False
    st_post_st_chart = settings.st_post_st_chart and success
    st_post_mt_chart = settings.st_post_mt_chart and success
    if(success):
        vx_continuous_df['VIX'] = vix_df['Close']

    if(st_post_st_chart):
        # Plot short-term VX data to image file.
        generate_vx_figure(vx_continuous_df, settings.st_years, 'VIX', 'STCMVF', 'VIX', 'Short-Term Constant-Maturity VIX Futures (STCMVF)', settings.st_histogram_xstep)
        plt.savefig(settings.st_st_chart_file, dpi=300)
    if(st_post_mt_chart):
        # Plot mid-term VX data to image file.
        generate_vx_figure(vx_continuous_df, settings.mt_years, 'VIX', 'MTCMVF', 'VIX', 'Mid-Term Constant-Maturity VIX Futures (MTCMVF)', settings.mt_histogram_xstep)
        plt.savefig(settings.st_mt_chart_file, dpi=300)

    # Dump continuous futures dataframe to Excel.
    write_vx_continuous_df_to_excel(vx_continuous_df, dry_run=(not settings.export_excel))

    # Update Excel file on Google Drive.
    update_vx_continuous_df_googledrive(dry_run=(not settings.export_excel))

    # Get recent VX quotes.
    vx_yesterday     = vx_continuous_df.iloc[-2]
    vx_today         = vx_continuous_df.iloc[-1]
    stcmvf_yesterday = vx_yesterday['STCMVF']
    stcmvf_today     = vx_today['STCMVF']
    stcmvf_percent   = (stcmvf_today / stcmvf_yesterday) - 1.0
    vix              = vx_today['VIX']
    stcmvf_premium   = (stcmvf_today / vix) - 1.0
    stcmvf_rate      = abs(stcmvf_premium) / 30.0
    stcmvf_verb      = 'charging' if stcmvf_premium > 0 else 'paying'
    mtcmvf_yesterday = vx_yesterday['MTCMVF']
    mtcmvf_today     = vx_today['MTCMVF']
    mtcmvf_percent   = (mtcmvf_today / mtcmvf_yesterday) - 1.0
    m4_vx            = vx_today['Month4 Settle']
    m4_weight        = vx_today['MT Month4 Weight']
    mtcmvf_premium   = (mtcmvf_today / vix) - 1.0
    mtcmvf_rate      = abs((mtcmvf_today / m4_vx) - 1.0) / (30.0 * (2.0 - m4_weight * 3.0))
    mtcmvf_verb      = 'charging' if mtcmvf_today > m4_vx else 'paying'
    logger.debug('vx_yesterday =\n{}'.format(vx_yesterday))
    logger.debug('vx_today =\n{}'.format(vx_today))

    # Post to StockTwits.
    st_st_message = settings.st_st_message.format(stcmvf_today, stcmvf_percent, stcmvf_premium, vix, stcmvf_verb, stcmvf_rate)
    st_mt_message = settings.st_mt_message.format(mtcmvf_today, mtcmvf_percent, mtcmvf_premium, vix, mtcmvf_verb, mtcmvf_rate)
    logger.debug('st_st_message = {}'.format(st_st_message))
    logger.debug('st_mt_message = {}'.format(st_mt_message))

    if(st_post_st_chart):
        st_st_attachment = settings.st_st_chart_file
        logger.debug('Posting message with {}.'.format(settings.st_st_chart_file))
    else:
        st_st_attachment = None
    if(st_post_mt_chart):
        st_mt_attachment = settings.st_mt_chart_file
        logger.debug('Posting message with {}.'.format(settings.st_mt_chart_file))
    else:
        st_mt_attachment = None

    post_to_stocktwits(
        settings.st_access_token,
        st_st_message,
        attachment=st_st_attachment,
        dry_run=settings.st_dry_run
        )
    post_to_stocktwits(
        settings.st_access_token,
        st_mt_message,
        attachment=st_mt_attachment,
        dry_run=settings.st_dry_run
        )
#END: main

def generate_vx_figure(vx_continuous_df, years, column_a, column_b, title_a, title_b, histogram_xstep):
    """
    Create the continuous VX figure, which plots column A and column B over time, the
    percent difference between the two, and their histograms.

    Parameters
    ----------
    vx_continuous_df : pd.DataFrame
        Dataframe generated from cboe.build_continuous_vx_dataframe with an
        added column, 'VIX', that represents VIX's values.

    years : int
        Number of years over which to plot and sample.

    column_a : str
        Name of first column in vx_continuous_df to plot.

    column_b : str
        Name of second column in vx_continuous_df to plot.

    title_a : str
        Title of first data-series in vx_continuous_df to plot.

    title_b : str
        Title of second data-series in vx_continuous_df to plot.

    histogram_xstep : float
        Volatility histogram's x-axis step value.
    """
    data_a = vx_continuous_df[cboe.today-years*365*cboe.Day():][column_a].dropna()
    data_b = vx_continuous_df[cboe.today-years*365*cboe.Day():][column_b].dropna()

    # Setup a grid of sub-plots.
    fig = plt.figure(1)
    gs  = gridspec.GridSpec(2, 2, height_ratios=[2, 1], width_ratios=[2, 1])
    plt.suptitle('{} vs {}'.format(title_a, title_b), style='italic', fontweight='bold', color='#707070')

    # data_a vs data_b
    timeseries_axes1 = plt.subplot(gs[0, 0])
    timeseries_axes1.plot(data_a, label=column_a)
    timeseries_axes1.plot(data_b, label=column_b, alpha=0.75)
    plt.setp(timeseries_axes1.get_xticklabels(), visible=False) # hide date labels on top subplot
    plt.grid(True)
    plt.ylabel('Volatility Level')
    plt.title('{:0.0f}-Year Daily Chart'.format(years))
    xs, xe = timeseries_axes1.get_xlim()
    logger.debug('xs, xe = {}, {}'.format(xs, xe))
    plt.annotate('{:0.3f}'.format(data_b[-1]), xy=(data_b.index[-1], data_b[-1]), xytext=(xe+(xe-xs)*0.03, data_b[-1]),
            verticalalignment='center', arrowprops=dict(arrowstyle='-', color='#ff9f4b'), color='#ff9f4b')
            # show last value

    # Percent difference between data_b and data_a
    pct_diff = ((data_b / data_a) - 1.0) * 100.0
    timeseries_axes2 = plt.subplot(gs[1, 0], sharex=timeseries_axes1)
    timeseries_axes2.plot(pct_diff, 'k-')
    plt.grid(True)
    xs, xe = timeseries_axes2.get_xlim()
    logger.debug('xs, xe = {}, {}'.format(xs, xe))
    ys, ye = timeseries_axes2.get_ylim()
    ystep  = 10.0
    logger.debug('ys, ye (before)= {}, {}'.format(ys, ye))
    ys = np.sign(ys)*np.floor(np.abs(ys)/ystep)*ystep # round-down to nearest ystep
    ye = np.sign(ye)*np.ceil(np.abs(ye)/ystep)*ystep # round-up to nearest ystep
    logger.debug('ys, ye (after)= {}, {}'.format(ys, ye))
    plt.yticks(np.arange(ys, ye, ystep)) # set rounded y-values stepped by ystep
    plt.ylabel('{}-{} (%)'.format(column_b, column_a))
    plt.annotate('{:0.1f}%'.format(pct_diff[-1]), xy=(pct_diff.index[-1], pct_diff[-1]), xytext=(xe+(xe-xs)*0.03, pct_diff[-1]),
            verticalalignment='center', arrowprops=dict(arrowstyle='-'))
            # show last percent-difference value

    # Histograms
    hist_axes = plt.subplot(gs[0, 1])
    hist_axes.hist(data_a, bins='auto', label=column_a)
    hist_axes.hist(data_b, bins='auto', label=column_b, alpha=0.75)
    plt.grid(True, axis='x')
    plt.setp(hist_axes.get_yticklabels(), visible=False) # hide occurence labels on histogram
    plt.setp(hist_axes.get_yticklines(),  visible=False) # hide occurence axis lines on histogram
    xs, xe = hist_axes.get_xlim()
    xstep  = histogram_xstep
    xclamp_a = np.min(data_a)
    xclamp_b = np.min(data_b)
    xclamp   = np.min([xclamp_a, xclamp_b])
    logger.debug('xs, xe (before)= {}, {}'.format(xs, xe))
    xs = np.max([xclamp, xs])
    xs = np.sign(xs)*np.floor(np.abs(xs)/xstep)*xstep # round-down to nearest xstep
    xe = np.sign(xe)*np.ceil(np.abs(xe)/xstep)*xstep # round-up to nearest xstep
    logger.debug('xs, xe (after)= {}, {}'.format(xs, xe))
    plt.xticks(np.arange(xs, xe, xstep)) # set rounded x-values stepped by xstep
    plt.xlabel('Volatility Level')
    plt.title('Histogram')
    plt.legend(bbox_to_anchor=(0.5, -0.25), loc='upper center', ncol=1) # place legend below histogram

    # Minor adjustments
    plt.setp(timeseries_axes2.get_xticklabels(), rotation=60, # rotate dates along x-axis
            horizontalalignment='right')
    plt.subplots_adjust(bottom=0.17, hspace=0.10, wspace=0.35) # adjust spacing between and around sub-plots
#END: generate_vx_figure

def write_vx_continuous_df_to_excel(vx_continuous_df, filename='vf.xlsx', cache_dir='.data', dry_run=False):
    """
    Dump VIX futures continuous dataframe to an Excel file with formatting. Data is
    cached (see {cache_dir}/vx_continuous_df.p) and reused on each run, eliminating
    the need to rebuild old data.

    Parameters
    ----------
    vx_continuous_df : pd.Dataframe
        Dataframe containing the VIX futures continuous data.

    filename : str
        Name of Excel file to write to.

    cache_dir : str
        Directory in which to cache the dataframe.

    dry_run : bool
        Do not actually update.
    """
    if(dry_run):
        logger.debug('Dry-run is enabled so will not update.')
        return
    logger.debug('vx_continuous_df = \n{}'.format(vx_continuous_df))
    writer = ExcelWriter(filename, engine='openpyxl')
    cache_path = '{}/vx_continuous_df.p'.format(cache_dir)
    try:
        # Load continuous futures dataframe from cache.
        cache_vx_continuous_df = pickle.load(open(cache_path, 'rb'))
    except:
        cache_vx_continuous_df = vx_continuous_df
    # Update end of cache.
    cache_vx_continuous_df = cache_vx_continuous_df[
            cache_vx_continuous_df.index < vx_continuous_df.index[0]
            ]
    all_vx_continuous_df = pd.concat([cache_vx_continuous_df, vx_continuous_df])
    logger.debug('all_vx_continuous_df = \n{}'.format(all_vx_continuous_df))
    try:
        # Cache continuous futures dataframe.
        pickle.dump(all_vx_continuous_df, open(cache_path, 'wb'))
        logger.debug('Cached VIX futures continuous dataframe in ({}).'.format(cache_path))
    except:
        logger.exception('Failed to cache VIX futures continuous dataframe.')
    all_vx_continuous_df.to_excel(writer, sheet_name='Continuous')
    sheet = writer.sheets['Continuous']
    # Set column widths.
    for col in ('A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S'):
        sheet.column_dimensions[col].width = 25 # index/trade date
    # Set column formats.
    for col in ('C', 'E', 'F', 'G', 'H', 'I', 'P', 'Q'):
        for c in sheet[col]:
            c.number_format = '0.000'
    for col in ('J', 'K'):
        for c in sheet[col]:
            c.number_format = '0'
    for col in ('L', 'M', 'N', 'O'):
        for c in sheet[col]:
            c.number_format = '0.0%'
    for col in ('R', 'S'):
        for c in sheet[col]:
            c.number_format = '0.00'
    writer.save()
    logger.debug('Dumped continuous futures dataframe to ({}).'.format(filename))
#END: write_vx_continuous_df_to_excel

def update_vx_continuous_df_googledrive(filename='vf.xlsx', fileId='0B4HikxB_9ulBMk5KY0YzQ2tzdzA', dry_run=False):
    """
    Update Excel file on Google Drive containing the VIX futures continuous data.

    Parameter:
    ----------
    filename : str
        Name of Excel file to write to.

    fileId : str
        File's Google Drive ID.

    dry_run : bool
        Do not actually update.
    """
    if(dry_run):
        logger.debug('Dry-run is enabled so will not update.')
        return

    from googledrive import get_credentials, update_file
    import httplib2
    from apiclient import discovery
    from googleapiclient.http import MediaFileUpload
    from oauth2client import client, tools
    from oauth2client.file import Storage

    # Authorize
    credentials = get_credentials('VIX Futures Data', consent=False)
    if(not credentials):
        raise Exception('Failed to retrieve credentials')
    http_auth = credentials.authorize(httplib2.Http())
    drive_service = discovery.build('drive', 'v3', http=http_auth, cache_discovery=False)

    # Upload
    update_file(
        drive_service,
        fileId=fileId,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        filename=filename
        )
#END: update_vx_continuous_df_googledrive

def post_to_stocktwits(access_token, message, link_preamble=' ', link=None, attachment=None, dry_run=False):
    """
    Post message and attachment (optional) to StockTwits using the given access token
    (see https://stocktwits.com/developers/docs/authentication). Messages must be
    less than 1000 characters.

    Parameters
    ----------
    access_token : str
        Token generated from StockTwits used for authentication.

    message : str
        Message to be posted.

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

    if(total_count > 1000):
        logger.error('Message length, {}, exceeds 1000 characters.'.format(total_count))

    if(dry_run):
        logger.debug('Dry-run is enabled so will not post.')
        return
    posted = False
    while not posted:
        try:
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
            else:
                posted = True
        except:
            logger.exception('Failed to post to StockTwits.')
            time.sleep(1)

    logger.info('Posted message: ' + message)
#END: post_to_stocktwits

if(__name__ == '__main__'):
    main()
