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
# # Archiving your jobs
#
# If you submit several large queries you may go over quota: set to 100 GB. In order to avoid to get over quota you may consider archiving your jobs. Archiving removes the data from the server side but keeps the SQL query. This allows to resubmit a query at a later time.
#
# Deleting (Archiving) a job with `pyvo` can be simply done that way:

job.delete()

# ## Archiving all `COMPLETED` jobs
#
# A nice feature of the `TAP service` is to retrieve all jobs that are marked as `COMPLETED` and archive them at ones. This can be done as follows:

#
# Archiving all COMPLETED jobs
#

# obtain the list of completed job_descriptions
completed_job_descriptions = tap_service.get_job_list(phases='COMPLETED')

# Archiving each of them
for job_description in completed_job_descriptions:
    
    # get the jobid
    jobid = job_description.jobid
    
    # recreate the url by hand
    job_url = tap_service.baseurl + '/async/' + jobid
    
    # recreate the job
    job = pyvo.dal.AsyncTAPJob(job_url, session=tap_session)
    
    print('Archiving: {url}'.format(url=job_url))
    job.delete() # archive job

# ## Rerunning `ARCHIVED` jobs
#
# Rerunning and retrieving results from a job that have been archived previously, can be achieved that way:

#
# Rerunning Archived jobs
#

# obtain the list of the two last ARCHIVED job_descriptions
archived_job_descriptions = tap_service.get_job_list(phases='ARCHIVED', last=2)

# rerunning the two last Archived jobs
for job_description in archived_job_descriptions:
    
    # get jobid
    jobid = job_description.jobid
    
    # recreate the url by hand
    job_url = tap_service.baseurl + '/async/' + jobid
    
    # recreate the archived job
    archived_job = pyvo.dal.AsyncTAPJob(job_url, session=tap_session)

    # get the language (with a bit of magic)
    lang = [parameter._content for parameter in archived_job._job.parameters if parameter._id == 'query_language'][0]

    # extract the query
    query = archived_job.query
    
    # resubmit the query with corresponding parameters
    job = tap_service.submit_job(query, language=lang, runid='rerun', queue='1m')
    print('%(url)s :\n%(query)s\n' % {"url": job_url, "query": query})
    
    # start the job
    try:
        job.run()
    except pyvo.dal.DALServiceError:
        raise ValueError("Please check that the SQL query is valid, and that the SQL language is correct.")    

# Retrieving the results is done alike explained above.
#
# If you prefer you can also filter for a given `runid`.

#
# Filtering by runid
#

target_runid = 'radial_prof_massive_bdmv'

# obtain the list of completed job_descriptions
archived_job_descriptions = tap_service.get_job_list(phases='ARCHIVED')

for job_description in archived_job_descriptions:
    
    # select the job with runid fitting target_runid
    if job_description.runid == target_runid:
        
        # get jobid
        jobid = job_description.jobid
    
        # recreate the url by hand
        job_url = tap_service.baseurl + '/async/' + jobid
    
        # recreate the archived job
        archived_job = pyvo.dal.AsyncTAPJob(job_url, session=tap_session)

        # get the language (with a bit of magic)
        lang = [parameter._content for parameter in archived_job._job.parameters if parameter._id == 'query_language'][0]

        # extract the query
        query = archived_job.query
    
        # resubmit the query with corresponding parameters
        job = tap_service.submit_job(query, language=lang, runid='rerun', queue='1m')
        print('%(url)s :\n%(query)s\n' % {"url": job_url, "query": query})
    
        # start the job
        try:
            job.run()
        except pyvo.dal.DALServiceError:	
            raise ValueError("Please check that the SQL query is valid, and that the SQL language is correct.")
