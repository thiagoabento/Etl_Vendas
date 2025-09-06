import pandas as pd
from etl.transform import incluir_coluna_total, ajustando_valor_negativo



def test_transform_data_return_colum_total():
    df = pd.DataFrame({
        'quantidade': [1, 2, 1],
        'preco': [10, 15, 30]
    })

    df_result = incluir_coluna_total(df)

    assert 'total' in df_result.columns
    assert df_result['total'].tolist() == [10, 30, 30]

def test_ajustando_valor_negativo():
    df = pd.DataFrame({

        'quantidade' : [-2,3,-4,6,5]

    })

    valores_esperados = [2.0, 3.0, 4.0, 6.0, 5.0]

    df_result = ajustando_valor_negativo(df)

    assert df_result['quantidade'].tolist() == valores_esperados




