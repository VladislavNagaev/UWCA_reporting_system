import yaml

from .csv_to_parquet import csv_to_parquet
from .prepare_file_path import prepare_file_path


def interim_processing_0(
    path_to_source_file:str, 
    path_to_result_file:str, 
    path_to_processing_params:str, 
    **kwargs,
) -> bool:

    try:
        
        # Загрузка словаря параметро в формате yaml
        processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))

        # Подготовка директории для сохранения файла
        file_path_status = prepare_file_path(path_to_file=path_to_result_file)

        processing_status = csv_to_parquet(
            path_to_csv_file=path_to_source_file, 
            path_to_parquet_file=path_to_result_file, 
            params=processing_params,
        )

    except Exception:
        
        # Статус обработки
        processing_status = False
    
    return processing_status