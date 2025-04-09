"""
Flask application initialization module.
Creates the flask app initializes the threadpool and dataingestor.
"""
import os
import logging
import time
from logging.handlers import RotatingFileHandler
from flask import Flask

# disable werkzeug request logs
log = logging.getLogger('werkzeug')
log.setLevel(logging.ERROR)

# setup main logger
logger = logging.getLogger('webserver')
logger.setLevel(logging.INFO)

# 1mb size limit
handler = RotatingFileHandler('webserver.log', maxBytes=1024*1024, backupCount=5)
handler.setLevel(logging.INFO)

def timetz(*args):
    """Return gmtime which forces UTC time"""
    return time.gmtime()

logging.Formatter.converter = timetz

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

if not os.path.exists('results'):
    os.mkdir('results')

webserver = Flask(__name__)

from app.data_ingestor import DataIngestor
from app.task_runner import ThreadPool

webserver.logger = logger
webserver.tasks_runner = ThreadPool()
webserver.data_ingestor = DataIngestor("./nutrition_activity_obesity_usa_subset.csv")
webserver.job_counter = 1

from app import routes
