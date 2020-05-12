.. Created by log.py at 2020-05-12, command
   '/Users/eileenwork/development/work/lapis/venv/lib/python3.7/site-packages/change/__main__.py log docs/source/changes compile --output docs/source/changelog.rst'
   based on the format of 'https://keepachangelog.com/'
#########
ChangeLog
#########

0.4 Series
==========

Version [0.4.0] - 2020-05-12
++++++++++++++++++++++++++++

* **[Added]** Basic documentation
* **[Added]** Changelog
* **[Added]** Information about input files for jobs
* **[Added]** Drone as a requirement to run a job

* **[Changed]** Standardisation of units

* **[Fixed]** Duplicate registration of drones
* **[Fixed]** Handling of black for pypy
* **[Fixed]** Proper termination of simulation
* **[Fixed]** Jobs execution within drones
* **[Fixed]** Scheduling of jobs
* **[Fixed]** Cancelation of jobs
* **[Fixed]** Importing of HTCondor jobs

0.3 Series
==========

Version [0.3.0] - 2019-10-27
++++++++++++++++++++++++++++

* **[Added]** Pre-commit hooks

* **[Changed]** Object-based logging and logging for job events

* **[Fixed]** Proper termination of simulation
* **[Fixed]** Update of available resources during scheduling cycle

0.2 Series
==========

Version [0.2.0] - 2019-10-25
++++++++++++++++++++++++++++

* **[Changed]** Support of current API of usim
* **[Changed]** Rename from lapis to lapis-sim for pypi and rtd

0.1 Series
==========

Version [0.1.1] - 2019-10-24
++++++++++++++++++++++++++++

* **[Added]** Requirement for flake8

* **[Changed]** Support of current API of usim
* **[Changed]** Distribution setup and license information
* **[Changed]** Cleanup and improvements of existing code
* **[Changed]** Extension of logging

* **[Fixed]** Termination of simulation
* **[Fixed]** Calculation of used and requested resource ratio
* **[Fixed]** StopIteration handling by Job Generator
* **[Fixed]** Importing of SWF files
