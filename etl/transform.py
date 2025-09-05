import pandas as pd
import logging

from .utils import connection_banco_da_dados


def test_transform_data(df):
    try:
        df = convertendo_literal_em_numeral(df)
        df = ajustando_valor_negativo(df)
        df = incluir_coluna_total(df)
        df = retirar_campos_nulos(df)
        df = padronizar_campo_data(df)
        logging.info('Transformação de dados concluída com sucesso...')
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função de transformar os dados: {e}')


def retirar_campos_nulos(df):
    try:
        df = df.dropna()
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função "retirar_campos_nulos": {e}')


def ajustando_valor_negativo(df):
    try:
        df.loc[:, 'quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce')
        df.loc[:, 'quantidade'] = df['quantidade'].apply(
            lambda x: abs(float(x)) if float(x) < 0 else float(x)
        )
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função "ajustando_valor_negativo": {e}')


def convertendo_literal_em_numeral(df):
    try:
        literal = {
            'um': 1, 'dois': 2, 'três': 3, 'quatro': 4, 'cinco': 5,
            'seis': 6, 'sete': 7, 'oito': 8, 'nove': 9,
            'mil': 1000
        }

        if df['quantidade'].dtype == 'object':
            df['quantidade'] = df['quantidade'].apply(lambda x: literal.get(x, x))
            df['quantidade'] = pd.to_numeric(df['quantidade'], errors='coerce')

        if df['preco'].dtype == 'object':
            df['preco'] = df['preco'].apply(lambda x: literal.get(x, x))

        literal_quantidade = df['quantidade'].apply(
            lambda x: isinstance(x, str) and x in literal.keys()
        ).any()

        literal_preco = df['preco'].apply(
            lambda x: isinstance(x, str) and x in literal.keys()
        ).any()

        if literal_quantidade or literal_preco:
            logging.warning('Ainda existem literais não convertidos na coluna "quantidade / preço".')

        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função "convertendo_literal_em_numeral": {e}')
        return None


def padronizar_campo_data(df):
    try:
        if 'data_venda' in df.columns:
            df = df.copy()
            df['data_venda'] = df['data_venda'].astype(str)
            df['data_venda'] = pd.to_datetime(df['data_venda'], errors='coerce')
            df['data_venda'] = df['data_venda'].dt.strftime('%d/%m/%y')
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função "padronizar_campo_data": {e}')
        return df


def incluir_coluna_total(df):
    try:
        df['quantidade'] = df['quantidade'].astype(float)
        df['preco'] = df['preco'].astype(float)
        df['total'] = df['quantidade'] * df['preco']
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro na função "incluir_coluna_total": {e}')


def incluir_dados_agrupados_estado():
    try:
        conn = connection_banco_da_dados('sqlite')
        df = pd.read_sql('select * from vendasecommerce', conn)
        total_por_estado = df.groupby('estado')['total'].sum().reset_index()
        return total_por_estado
    except Exception as e:
        logging.error(f'Ocorreu erro na função "incluir_dados_agrupados_estado": {e}')


def calcula_ticket_medio():
    try:
        conn = connection_banco_da_dados('sqlite')
        df = pd.read_sql('select preco from vendasecommerce', conn)
        ticket_medio = round(df['preco'].mean(), 2)
        media = pd.DataFrame({'ticket_medio': [ticket_medio]})
        return media
    except Exception as e:
        logging.error(f'Ocorreu erro na função "calcula_ticket_medio": {e}')


def calcula_total_por_produto():
    try:
        conn = connection_banco_da_dados('sqlite')
        df = pd.read_sql('select * from vendasecommerce', conn)
        total_por_produto = df.groupby('produto')['total'].sum().reset_index()
        return total_por_produto
    except Exception as e:
        logging.exception(f'Ocorreu erro na função "calcula_total_por_produto": {e}')
