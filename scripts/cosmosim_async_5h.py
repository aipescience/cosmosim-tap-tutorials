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
# ## The 5 hours queue
#
# Some complex queries like Cross-Matching or geometric search may take more than the short queues allow. For this purpose we provide the **5 hours queue**. If you need longer queues please contact us. 
#
# When running a long query, you surely don't want to block CPU ressources for a python process that just wait for 5 hours, for the queue to finish. Therefore long queries are typically done in two parts (= two scripts), one that submits the request, another one that retrieve the results.
#
# ### Submitting a job and store `job_urls` for later retrieval
#
# We first submit the query as an async job to the `5h` queue, and store the job (the url) of the newly created job into a file `job_url.txt`. With this url we are able to retrieve the results (when it has finished) at any later time.

#
# Submit the query as an async job
#
query_name = ""
lang = 'PostgreSQL'

query = '''
-- Mass accression history of a halo
SELECT p.foftreeid, p.treesnapnum, p.mass, p.np
  FROM mdr1.fofmtree AS p, 
       (SELECT foftreeid, mainleafid FROM mdr1.fofmtree 
         WHERE fofid=85000000000) AS mycl
 WHERE p.foftreeid BETWEEN mycl.foftreeid AND mycl.mainleafid
 ORDER BY p.treesnapnum
'''

job = tap_service.submit_job(query, language=lang, runid=query_name, queue="5h")
job.run()

print('JOB %s: SUBMITTED' % (job.job.runid,))
print('JOB %s: %s' % (job.job.runid, job.phase))

#
# Save the job's url in a file to later retrieve results.
#
print('URL: %s' % (job.url,))

with open('job_url.txt', 'w') as fd:
    fd.write(job.url)

# ### Retrieve the results at a later time
#
# In order to retrieve the results, we will first recreate the job from the `job_url` stored in the `job_url.txt` file and verify that the job is finished, by asking for its current phase. In case the job is finished we will retrieve the results as usual.

#
# Recreate the job from url and session (token)
#

# read the url
with open('job_url.txt', 'r') as fd:
    job_url = fd.readline()

# recreate the job 
job = pyvo.dal.AsyncTAPJob(job_url, session=tap_session)

#
# Check the job status
#
print('JOB %s: %s' % (job.job.runid, job.phase))

# if still running --> exit
if job.phase not in ("COMPLETED", "ERROR", "ABORTED"):
    exit(0)

#
# Fetch the results
#
job.raise_if_error()
print('\nfetching the results...')
tap_results = job.fetch_result()
print('\n...DONE\n')

# Thanks to this method you can submit a job, go for a coffee, write a paper and retrieve the results 
# when it suits you. The job and its results are stored on the server side under your user account.
#
# The entire scripts can be here: [cosmosim-async-submit-5h.py](files/cosmosim-async-submit-5h.py) and [cosmosim-async-retrieve-5h.py](files/cosmosim-async-retrieve-5h.py)
#
