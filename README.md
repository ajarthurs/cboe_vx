# autoassets
CBOE VX Analytics

Fetches and processes VIX futures data from CBOE. It utilizes the pandas Dataframe [^1] to produce continuous contract data.

# Requirements:
* Python 3.10 or greater
* Pandas 1.4 or greater

# Installation:
TODO: Create `setup.py`

# Getting Started:
```bash
cp cbox_vx/templates/settings.py ${PWD}
cp cbox_vx/templates/logging.conf ${PWD}

# Edit `settings.py`.

python cboe_vx/bin/post.py
```
The program will download VIX futures contract data, generate continuous contracts, and cache results locally plus store charts to file.

# Status:
* This project is currently in alpha, meaning that major dependency-breaking changes will happen without notice. The user should keep a copy that works for their setup before upgrading.
* There is no frontend. The user must edit Python code by hand or otherwise develop their own frontend.

[^1]: https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html
