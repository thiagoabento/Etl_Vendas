
import pandas as pd
from etl.transform import transform_data

def test_transform_data():
    data = {'id': [1, 2, 3], 'nome': ['Ana', 'Bruno', 'Carlos'], 'idade': [25, 35, 40]}
    df = pd.DataFrame(data)
    transformed_df = transform_data(df)
    assert all(transformed_df['idade'] > 30)
    assert len(transformed_df) == 2
