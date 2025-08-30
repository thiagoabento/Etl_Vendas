from .utils import connection_banco_da_dados
import logging

def load_data_vendasecommerce(df, table_name='vendasecommerce'):
    try:
        conn = connection_banco_da_dados('sqlite')
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f'{len(df)} Registros carregados na tabela {table_name}')
    except Exception as e:
        logging.error(f'Erro ao carregar os dados: {e}')
    finally:
        conn.close()

def load_data_total_por_estado(df, table_name='totalestado'):
    try:
        conn = connection_banco_da_dados('sqlite')
        df.to_sql(table_name,conn,if_exists='replace',index=False)
        logging.info(f'{len(df)} Registros inseridos na tebala {table_name}')
    except Exception as e:
        logging.error(f'Ocorreu erro ao carregar dados na tabela totalestado: {e}')

    finally:
        conn.close()

def load_data_ticket_medio(df,  table_name='ticketmedio'):
    try:
        conn = connection_banco_da_dados('sqlite')
        df.to_sql(table_name, conn, if_exists='replace',index=False)
        logging.info(f'{len(df)} Registro inserido na tabela {table_name}')
    except Exception as e:
        logging.exception(f'Ocorreu erro ao carregar a tabela {table_name}: {e}')

    finally:
        conn.close()

def load_data_total_por_produto(df, table_name='totalproduto'):
    try:
        conn = connection_banco_da_dados('sqlite')
        df.to_sql(table_name, conn, if_exists='replace', index=False)
        logging.info(f'{len(df)} Registros inseridos na tabela {table_name}')
    except Exception as e:
        logging.error(f'Ocorreu erro ao carregar dados na tebela {table_name} : {e}')

    finally:
        conn.close()





