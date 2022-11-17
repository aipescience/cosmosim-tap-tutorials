# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: nomarker
#       format_version: '1.0'
#       jupytext_version: 1.13.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# # Installation of pyvo
# ---
#
# In order to interact with the TAP interface of `www.cosmosim.org` you only require
# `python 3+` and `pyvo 1+`.

# pip install pyvo>=1.0

# # Importing PyVo and checking the version
# ---
#
# It is useful to always print the version of pyvo you are using. Most of non-working scripts fail because of an old version of `pyvo`.

from pkg_resources import parse_version
import pyvo

#
# Verify the version of pyvo
#
if parse_version(pyvo.__version__) < parse_version('1.0'):
    raise ImportError('pyvo version must be at least than 1.0')

print('\npyvo version %s \n' % (pyvo.__version__,))
# # Authentication
# ---
#
# After registration you can access your API Token by clicking on your user name in the right side of the menu bar. Then select `API Token`. 
#
# ![aip-token](files/cosmosim-api-token-menu.png)
#
# You will see a long alphanumerical word. Just copy it where ever you see `<your-token>` ; in the following examples. 
#
# ![aip-token-blured](files/cosmosim-api-token-page.png)
#
# > **The `API Token` identifies you and provides access to the results tables of your queries.**
#
# The connection to the TAP service can be done that way:

import requests
import pyvo

#
# Setup tap_service connection
#
service_name = "CosmoSim"
url = "https://www.cosmosim.org/tap"
token = 'Token <your-token>'

print('TAP service %s \n' % (service_name,))

# Setup authorization
tap_session = requests.Session()
tap_session.headers['Authorization'] = token

tap_service = pyvo.dal.TAPService(url, session=tap_session)
# ## The 1 minute queue
#
# Most of the asynchronous queries will require less than 1 minute, basically all queries without `JOIN`, or `CONE SEARCH`. Therefore this queue is the default and should be preferred.

#
# Submit the query as an async job
#
query_name = "select_snapshot_by_redshifts"
lang = 'PostgreSQL' # ADQL or PostgreSQL
query = '''
-- Select simulation snapshots by redishift
SELECT distinct zred, aexp, snapnum 
  FROM mdr1.redshifts 
 ORDER BY snapnum 
  DESC      
'''

job = tap_service.submit_job(query, language=lang, runid=query_name, queue="1m")
job.run()

#
# Wait to be completed (or an error occurs)
#
job.wait(phases=["COMPLETED", "ERROR", "ABORTED"], timeout=60.0)
print('JOB %s: %s' % (job.job.runid, job.phase))

#
# Fetch the results
#
job.raise_if_error()
print('\nfetching the results...')
tap_results = job.fetch_result()
print('...DONE\n')

# As for sync jobs, the result is a [`TAPResults`](https://pyvo.readthedocs.io/en/latest/api/pyvo.dal.TAPResults.html#pyvo.dal.TAPResults) object.
#
# The entire script can be found at: [cosmosim-async-1m.py](files/cosmosim-async-1m.py)
