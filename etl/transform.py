import pandas as pd
import logging


def transform_data(df):
    try:
        df = df[df['preco_unitario'] > 500]
        df = remove_null(df)
        df = formatar_data(df)
        df = incluir_coluna_total(df)
        df = incluir_observacao(df)
        logging.info(f'Transformação de dados concluído com sucesso...')
        return df
    except Exception as e:
        logging.error(f'Ocorreu erro no processo de transformação dos dados: {e}')
        raise

def remove_null(df):
    return df.dropna()

def incluir_coluna_total(df):
    df['total'] = round(df['preco_unitario'] * df['quantidade'],2)

    return df

def incluir_observacao(df):
    def classifica_venda(valor):
        if valor > 1000:
            return "Grande venda!"
        elif valor > 500:
            return "Venda média!"
        else:
            return "Pequena venda!"
    df['obsvenda'] = df['total'].apply(classifica_venda)
    return df

def formatar_data(df):
    df['data_pedido'] = pd.to_datetime(df['data_pedido'])
    df['data_pedido'] = df['data_pedido'].dt.strftime('%d/%m/%y')
    return df


