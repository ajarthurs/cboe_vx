#!/usr/bin/python
#
# Populate 'data.p' required by post_STCMVF.py

import pandas as pd
import logging
import pickle

print('Prompting for the prior expiration date and settlement prices.')
done = False
while(not done):
    s = input('Prior expiration date (YEAR MONTH DAY): ')
    try:
        prior_expdate = pd.to_datetime(s, format='%Y %m %d').date()
    except ValueError as e:
        logging.info(e)
    else:
        done = True
done = False
while(not done):
    s = input('Prior front-month settlement price: ')
    try:
        prior_front_month_price = float(s)
    except ValueError as e:
        logging.info(e)
    else:
        done = True
done = False
while(not done):
    s = input('Prior back-month settlement price: ')
    try:
        prior_back_month_price = float(s)
    except ValueError as e:
        logging.info(e)
    else:
        done = True

# Save persistent variables.
pickle.dump((\
        prior_expdate,\
        prior_front_month_price,\
        prior_back_month_price \
        ), open('data.p','wb'))
