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
# ## List of file queries
#
# Sometimes it is useful to just send all `.sql` queries present in a directory. For such purpose you can use comments to provide the proper parameters.
#
# Let us consider the file `radial_prof_massive_bdmv.sql`
#
# ```sql
# -- Radial profile of most massive BDMV (z=0)
#
# -- LANGUAGE = PostgreSQL
# -- QUEUE = 1h
#
# SELECT * FROM bolshoi.bdmvprof
#  WHERE bdmid =
#        (SELECT bdmid FROM bolshoi.bdmv
#          WHERE snapnum=416 ORDER BY mvir DESC LIMIT 1)
#  ORDER BY rbin
# ```
#
# The `language` and `queue` are prescibed as comments. The query can then be submitted in a script like the following:

import glob

#
# Submit the query as an Asynchrone job
#

# find all .sql files in current directory
queries_filename = sorted(glob.glob('./*.sql'))
print('Sending %d examples' % (len(queries_filename),))

# initialize test results
jobs = []
failed =  []

# send all queries
for query_filename in queries_filename:

    # read the .SQL file
    with open(query_filename, 'r') as fd:
        query = ' '.join(fd.readlines())

    # Set language from comments (default: PostgreSQL)
    if 'LANGUAGE = ADQL' in query:
        lang = 'ADQL'
    else:
        lang = 'PostgreSQL'

    # Set queue from comments (default: 1m)
    if 'QUEUE = 5h' in query:
        queue = "5h"
    elif 'QUEUE = 1h' in query:
        queue = "1h"
    elif 'QUEUE = 1m' in query:
        queue = "1m"
    else:
        queue = "1m"


    # Set the runid from sql filename
    base = os.path.basename(query_filename)
    runid = os.path.splitext(base)[0]
    
    print('\n> Query : %s\n%s\n' % (runid, query))

# The rest of the submission process and retrieval can be done in any manner. An example 
# can be found here: [cosmosim-tutorial-from-files.py](files/cosmosim-tutorial-from-files.py)
#
