# Various application settings.
#
# To set things up, do the following:
# 1. Copy this file to settings.py
# 2. Fill in the variables below

# Application settings.
st_years           = 2
st_histogram_xstep = 5.0
mt_years           = 5
mt_histogram_xstep = 10.0
check_for_holiday  = True
export_excel       = False


# StockTwits settings.
# (see https://stocktwits.com/developers/docs/authentication)
st_dry_run      = True
st_access_token = 'my-StockTwits-access-token'

# Short-term VX ETPs
st_st_message    = """STCMVF settled @ {:.3f} ({:+.1%}), {:+.1%} over VIX ({:.3f}). STCMVF is {} speculators {:.2%} per day.

The short-term constant-maturity VIX futures (STCMVF) is a weighted average of the current front- and back-month VIX futures contracts, and it resets daily. STCMVF represents the market's 30-day estimate of the S&P 500 30-day volatility index ($VIX), derived from $SPX options. Short-term VIX ETPs (e.g., $VXX, VIXY, SVXY, $UVXY, TVIX) approximately track STCMVF while compounding daily returns from prior positions."""
st_post_st_chart = True
st_st_chart_file = 'st_chart.png'

# Mid-term VX ETPs
st_mt_message    = """MTCMVF settled @ {:.3f} ({:+.1%}), {:+.1%} over VIX ({:.3f}). MTCMVF is {} speculators {:.2%} per day.

The mid-term constant-maturity VIX futures (MTCMVF) is a weighted average of the current 4th, 5th, 6th, and 7th month VIX futures contracts, and it resets daily. MTCMVF represents the market's 5-month estimate of the S&P 500 30-day volatility index ($VIX), derived from $SPX options. Mid-term VIX ETPs (e.g., $VXZ, VIXM, ZIV, VIIZ, TVIZ) approximately track MTCMVF while compounding daily returns from prior positions."""
st_post_mt_chart = True
st_mt_chart_file = 'mt_chart.png'
