from etl.extract import extract_data
from etl.transform import transform_data
from etl.transform import incluir_dados_agrupados_estado
from etl.transform import calcula_ticket_medio
from etl.transform import calcula_total_por_produto
from etl.load import load_data_vendasecommerce
from etl.load import  load_data_total_por_estado
from etl.load import load_data_ticket_medio
from etl.load import load_data_total_por_produto
from etl.utils import setup_logging


import logging


def run_etl():
     try:
          setup_logging()
          logging.info('## Iniciando ETL ##')

          df = extract_data()
          logging.info(f'{len(df)} registros extraídos')

          df_limpeza = transform_data(df)
          logging.info(f'{len(df_limpeza)} registros após transformação')

          load_data_vendasecommerce(df_limpeza)

          load_data_total_por_estado(incluir_dados_agrupados_estado())

          load_data_ticket_medio(calcula_ticket_medio())

          load_data_total_por_produto(calcula_total_por_produto())

          logging.info('## ETL finalizada ##')

     except Exception as e:
          logging.exception(f'Ocorreu erro no processo ETL: {e}')

if __name__ == "__main__":
    run_etl()

