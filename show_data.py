#!/usr/bin/python
#
# Display what's in 'data.p'

import pickle
import logging
import logging.config
import sys

# Log setup
logger = logging.getLogger('show_data')
logger.setLevel(logging.INFO)
fmt = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('show_data.log', 'w')
fh.setFormatter(fmt)
con = logging.StreamHandler(sys.stdout)
con.setFormatter(fmt)
logger.addHandler(fh)
logger.addHandler(con)

(\
        prior_expdate,\
        prior_front_month_price,\
        prior_back_month_price \
        ) = pickle.load(open('data.p','rb'))

logger.info('prior_expdate           = ' + str(prior_expdate          ))
logger.info('prior_front_month_price = ' + str(prior_front_month_price))
logger.info('prior_back_month_price  = ' + str(prior_back_month_price ))
