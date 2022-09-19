import pandas as pd
import yaml

from .allocate_flight_ranges import allocate_flight_ranges
from .prepare_file_path import prepare_file_path


def interim_processing_3(
    path_to_source_file:str, 
    path_to_result_file:str, 
    path_to_processing_params:str,
    **kwargs,
) -> bool:

    try:

        # Загрузка данных в формате parquet
        data = pd.read_parquet(
            path=path_to_source_file,
            engine='pyarrow'
        )

        # Загрузка словаря параметро в формате yaml
        processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))
        
        # Корректировка данных
        data = allocate_flight_ranges(data=data, params=processing_params).allocate_ranges()

        # Подготовка директории для сохранения файла
        file_path_status = prepare_file_path(path_to_file=path_to_result_file)

        # Сохранение результирующих данных
        data.to_parquet(
            path=path_to_result_file,
            compression='snappy',
            index=False,
            engine='pyarrow',
        )
        
        # Статус обработки
        processing_status = True
    
    except Exception:
        processing_status = False
    
    return processing_status



