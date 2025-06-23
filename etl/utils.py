
import sqlite3
import configparser
import logging
import os
from sqlalchemy import create_engine

config = configparser.ConfigParser()
config.read('config.ini')

def connection_banco_da_dados(db_type='name_bd'):
    if db_type == 'sqlite':
        return get_db_connection()
    elif db_type == 'postgres':
        return connection_postgree()
    else:
        raise ValueError(f'Banco de dados não configurado: {db_type}')

def get_db_connection():
    db_path = config['sqlite']['database']
    conn = sqlite3.connect(db_path)
    return conn

def connection_postgree():
    usuario = "postgres"
    senha = "1"
    host = "localhost"
    porta = "5432"
    banco = "vendas"
    url = f"postgresql+psycopg2://{usuario}:{senha}@{host}:{porta}/{banco}"
    engine = create_engine(url)
    return engine

def setup_logging():
    log_file = config['logging']['log_file']
    os.makedirs(os.path.dirname(log_file), exist_ok=True)
    logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')



