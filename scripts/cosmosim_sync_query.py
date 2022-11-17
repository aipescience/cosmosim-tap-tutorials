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
# # Short queries
# ---
#
# Many queries last less than a few seconds, we call them **short queries**. The latter can be executed with **synchronized** jobs. You will retrieve the results interactively.

lang = "PostgreSQL"

query = '''-- Select surrounding halos
SELECT bdmid, x, y, z, rvir, mvir 
  FROM mdr1.bdmv 
 WHERE pdist(1000, x,y,z, 998,450,500) < 5
 LIMIT 10
'''

tap_result = tap_service.run_sync(query, language=lang)

# > **Remark**:
# > the `lang` parameter can take two values either `PostgreSQL` or `ADQL`
# > this allows to access some featured present in the one or the other language 
# > for more details about the difference between both please refer :  [Documentation](http://tapvizier.u-strasbg.fr/adql/help.html) or to [IOVA docs](http://www.ivoa.net/documents/latest/ADQL.html)

# The result `tap_result` is a so called [`TAPResults`](https://pyvo.readthedocs.io/en/latest/api/pyvo.dal.TAPResults.html#pyvo.dal.TAPResults) that is essentially a wrapper around an Astropy `votable.Table`. For standard conversion see [Convert to various python types](#Convert-result-to-various-python-types).
#
# The entire script can be found on github: [cosmosim-sync-query.py](files/cosmosim-sync-query.py)
#
