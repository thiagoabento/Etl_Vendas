import logging
import os

def configuraLogJob():
    os.makedirs('logs', exist_ok=True)
    job_log = logging.getLogger('job_log_etl')
    job_log.setLevel(logging.INFO)
    format = logging.Formatter('%(asctime)s - (levelname)s - %(message)s')

    file_handler = logging.FileHandler('logs/job_etl.log')
    file_handler.setFormatter(format)

    #evita arquivos duplicados
    if not job_log.handlers:
        job_log.addHandler(file_handler)