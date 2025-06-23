import logging

import pandas as pd
import configparser


config = configparser.ConfigParser()
config.read('config.ini')

def extract_data():
    try:
        file_path = config['files']['input_file']
        df = pd.read_csv(file_path)
        logging.info(f'Arquivo processado com sucesso no caminho: {file_path}')
        return df
    except Exception as e:
        logging.error(f'Erro ao ler arquivo: {e}')
        raise

