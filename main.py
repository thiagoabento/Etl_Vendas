from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.load import total_por_estado
from etl.load import total_por_categoria
from etl.utils import setup_logging
from etl.load import save_total_vendas, save_total_por_estado, save_total_por_categoria
import logging


def run_etl():
     try:
          setup_logging()
          logging.info('-- Iniciando ETL --')

         # Extração
          df = extract_data()
          logging.info(f'{len(df)} registros extraídos')

         # Trasformação
          df_transformed = transform_data(df)
          logging.info(f'{len(df_transformed)} registros após transformação')

         # Carga de dados
          load_data(df_transformed)

         #Carga de dados postgree
          save_total_vendas(df_transformed)

          #Agrupa as vendas por estado
          save_total_por_estado(df)

          #Soma o total e agrupa por categoria
          save_total_por_categoria(df)

          logging.info('-- ETL Finalizada com sucesso --')

     except Exception as e:
          logging.info(f'Ocorreu erro no job ETL: {e}')

if __name__ == "__main__":
    run_etl()
