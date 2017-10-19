#!/usr/bin/python

"""Google Drive utility module."""

import httplib2
from apiclient import discovery
from googleapiclient.http import MediaFileUpload
from oauth2client import client, tools
from oauth2client.file import Storage
import os
import logging

logger = logging.getLogger(__name__)

def get_credentials(application, client_secret_file='client_secret.json', scopes='https://www.googleapis.com/auth/drive.metadata.readonly', store_dir='.store', consent=True, force_consent=False):
    """Get valid user credentials from storage.

    Parameters
    ----------
    application : str
        Name of application

    client_secret_file : str
        Name of JSON file containing the OAuth2 client ID.

    scopes: str
        Scope in which the client ID belongs.

    store_dir : str
        Name of directory containing credentials.

    consent : bool
        Open user's default web browser for consent, if needed. If False,
        raise an error when consent is needed.

    force_consent : bool
        Force user to consent. This option is useful if the user wants to
        change the client ID's scope.

    Returns
    -------
    oauth2client.client.Credentials
    """
    try:
        # Setup store directory
        os.mkdir(store_dir)
    except FileExistsError:
        pass
    except OSError:
        logger.exception('Failed to create store directory ({}).'.format(store_dir))
    # Fetch token from store.
    store_file = '{}/vix_futures_poster.json'.format(store_dir)
    store = Storage(store_file)
    credentials = store.get()
    if(force_consent or not credentials or credentials.invalid):
        if(consent):
            # Refresh token.
            flow = client.flow_from_clientsecrets(client_secret_file, scopes)
            flow.user_agent = application
            credentials = tools.run_flow(flow, store)
            logger.debug('Stored credentials in ({}).'.format(store_file))
        else:
            # Warn user that their consent is required.
            logger.error('Consent is required for authorization.')
    else:
        logger.debug('Fetched credentials from ({}).'.format(store_file))
    return(credentials)
#END: get_credentials

def update_file(drive_service, fileId, mimetype, filename, local_filename=None):
    if(not local_filename):
        local_filename = filename
    file_metadata = {
            'name' : filename,
            'mimeType' : mimetype
            }
    media = MediaFileUpload(
            local_filename,
            mimetype=mimetype,
            resumable=True
            )
    gd_file = drive_service.files().update(
            fileId=fileId,
            body=file_metadata,
            media_body=media,
            fields='name, parents'
            ).execute()
    logger.debug('Uploaded ({}) to Google Drive file ({}; parents={}).'.format(local_filename,
        gd_file.get('name'), gd_file.get('parents'))
        )
#END: update_file

def test_credentials():
    """Test unit that authenticates with Google Drvie."""
    import sys
    import code

    # Debug-level logging.
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s - %(name)s:%(funcName)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('google.test_credentials.log', 'w')
    fh.setFormatter(fmt)
    con = logging.StreamHandler(sys.stdout)
    con.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(con)

    # Authorize
    #credentials = get_credentials('VIX Futures Data', scopes='https://www.googleapis.com/auth/drive.file')
    credentials = get_credentials('VIX Futures Data', scopes='https://www.googleapis.com/auth/drive', force_consent=True)
    if(not credentials):
        raise Exception('Failed to retrieve credentials')
    http_auth = credentials.authorize(httplib2.Http())
    drive_service = discovery.build('drive', 'v3', http=http_auth)

    # Drop into a Python shell with all definitions.
    code.interact(local=dict(globals(), **locals()))

    # Test done. Reset logging.
    logger.setLevel(logging.WARNING)
#END: test_credentials

def test_upload():
    """Test unit that authenticates and uploads to Google Drive."""
    import sys
    import code

    # Debug-level logging.
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter('%(asctime)s - %(name)s:%(funcName)s - %(levelname)s - %(message)s')
    fh = logging.FileHandler('google.test_upload.log', 'w')
    fh.setFormatter(fmt)
    con = logging.StreamHandler(sys.stdout)
    con.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(con)

    # Authorize
    credentials = get_credentials('VIX Futures Data', consent=False)
    if(not credentials):
        raise Exception('Failed to retrieve credentials')
    http_auth = credentials.authorize(httplib2.Http())
    drive_service = discovery.build('drive', 'v3', http=http_auth)

    # Upload
    update_file(drive_service,
            fileId='0B4HikxB_9ulBMk5KY0YzQ2tzdzA',
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            filename='vf.xlsx'
            )

    # Drop into a Python shell with all definitions.
    code.interact(local=dict(globals(), **locals()))

    # Test done. Reset logging.
    logger.setLevel(logging.WARNING)
#END: test_upload

if(__name__ == '__main__'):
    #test_credentials()
    test_upload()
