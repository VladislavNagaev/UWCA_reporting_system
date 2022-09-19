import pandas as pd
from typing import List, Tuple, Literal, Optional


def csv_to_parquet(path_to_csv_file:str, path_to_parquet_file:str, params:dict,):
    """
    Парсит данные CSV-файла. Удаляет пустые строки. Удаляет недопустимые символы в наименованиях столбцов. 
    Сохраняет данные в бинарном формате parquet.
    
    path_to_csv_file - путь к исходному csv-файлу
    path_to_parquet_file - путь результирующему parquet-файлу
    """

    encoding = params.get('encoding')
    skiprows = params.get('skiprows')
    replace_symbols = params.get('replace_symbols')

    # Загрузка данных в формате CSV
    data = pd.read_csv(
        path_to_csv_file, 
        sep=';', 
        engine='c', 
        decimal=',', 
        encoding=encoding, 
        skiprows=skiprows,
    )

    # Remove unsupported symbols
    if replace_symbols is not None:
        new_columns = []
        for column in data.columns:
            column = column.strip()
            for replace_symbol in replace_symbols:
                column = column.replace(*replace_symbol)
            new_columns.append(column)
        data.columns = new_columns

    data.to_parquet(
        path=path_to_parquet_file,
        compression='snappy',
        index=False,
        engine='pyarrow',
    )
    
    return True

