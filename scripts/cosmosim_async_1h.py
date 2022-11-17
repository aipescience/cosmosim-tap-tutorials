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
# ## The 1 hour queue
#
# If you want to extract information on specific stars from various tables you have to `JOIN` tables. Your query may need more than a few seconds. For that, the **1 hour queue** provide a good balance. It should be noticed that for such a queue the wait method should not be used to prevent an overload of the server at peak usage. Therefore using the script with the `sleep()` method is recommended.

#
# Submit the query as an async job
#
lang = 'PostgreSQL'
query_name = "radial_prof_massive_bdmv"

query = '''
-- Radial profile of most massive BDMV (z=0)
SELECT * FROM bolshoi.bdmvprof
 WHERE bdmid =
       (SELECT bdmid FROM bolshoi.bdmv
         WHERE snapnum=416 ORDER BY mvir DESC LIMIT 1)
 ORDER BY rbin
'''

job = tap_service.submit_job(query, language=lang, runid=query_name, queue="1h")
job.run()

print('JOB %s: SUBMITTED' % (job.job.runid,))

#
# Wait for the query to finish
#
while job.phase not in ("COMPLETED", "ERROR", "ABORTED"):
    print('WAITING...')
    time.sleep(3600.0) # do nothing for some time

print('JOB ' + (job.phase))

#
# Fetch the results
#
job.raise_if_error()
print('\nfetching the results...')
results = job.fetch_result()
print('...DONE\n')

# The entire script can be found on github: [cosmosim-async-1h.py](files/cosmosim-async-1h.py)
