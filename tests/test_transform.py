import pandas as pd
from etl.transform import incluir_coluna_total


def test_transform_data_return_colum_total():
    df = pd.DataFrame({
        'quantidade': [1, 2, 3],
        'preco': [10, 15, 30]
    })

    df_result = incluir_coluna_total(df)

    assert 'total' in df_result.columns
    assert df_result['total'].tolist() == [10, 30, 90]



