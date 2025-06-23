import pandas as pd
from .utils import connection_banco_da_dados
import logging

def load_data(df, table_name='vendas'):
    conn = connection_banco_da_dados('sqlite')
    try:
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f'{len(df)} registros carregados na tabela {table_name}')
    except Exception as e:
        logging.error(f'Erro ao carregar os dados: {e}')
    finally:
        conn.close()

def total_por_estado():
    conn = connection_banco_da_dados('sqlite')
    cursor = conn.cursor()
    try:
        cursor.execute('DROP TABLE IF EXISTS resuvendasest')
        cursor.execute('CREATE TABLE resuvendasest(estado TEXT, total REAL)')

        cursor.execute('INSERT INTO resuvendasest(estado, total) SELECT estado, SUM(total) FROM vendas GROUP BY estado')
        conn.commit()

        logging.info("Cálculo de total por estado executado com sucesso!!")

    except Exception as e:
        logging.info(f'Erro no processo: {e}')
    finally:
        conn.close()
        logging.info(f'Conexão finalizada...')


def total_por_categoria():
    conn = connection_banco_da_dados('sqlite')
    cursor = conn.cursor()

    try:
        cursor.execute('DROP TABLE IF EXISTS resuvendascat')
        cursor.execute('CREATE TABLE resuvendascat(total REAL, categoria TEXT)')

        cursor.execute('INSERT INTO resuvendascat(total, categoria) SELECT SUM(total), categoria FROM vendas GROUP BY categoria')

        conn.commit()
        logging.info('Cálculo de total por categoria executado com sucesso!!')

    except Exception as e:
        logging.info(f'Ocorreu erro no processo: {e}')

    finally:
        logging.info(f'Conexão finalizada...')
        conn.close()


def save_total_vendas(df, table_name='vendas'):
    conn = connection_banco_da_dados('postgres')
    try:
        df.to_sql(table_name, conn, if_exists='replace',index=False)
        logging.info(f'Dados inseridos com sucesso na tabela {table_name} com {len(df)} registros')

    except Exception as e:
        logging.error(f"Ocorreu erro no processo: {e}")
        raise

def save_total_por_estado(df, table_name='resuvendasest'):
    conn = connection_banco_da_dados('postgres')
    try:
        df = pd.read_sql('select estado, total from vendas',conn)
        df = round(df.groupby('estado', as_index=False)['total'].sum(),2)
        df.to_sql(table_name,conn, if_exists='replace', index=False)
        logging.info(f'A tabela {table_name} foi criada e os dados inseridos totalizando {len(df)}')
    except Exception as e:
        logging.error(f'Ocorreu erro na inserção de dados agrupados na tabela {table_name}: {e}')
        raise

def save_total_por_categoria(df,table_name='resuvendascat'):
    conn = connection_banco_da_dados('postgres')
    try:
        df = pd.read_sql('select total, categoria from vendas',conn)
        df = round(df.groupby('categoria',as_index=False)['total'].sum(),2)
        df.to_sql(table_name,conn,if_exists='replace',index=False)
        logging.info(f'A tabela {table_name} foi criada com sucesso com {len(df)} registros')

    except Exception as e:
        logging.error(f'Ocorreu erro no processo total por categoria: {e}')
        raise


