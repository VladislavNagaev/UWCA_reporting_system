import pandas as pd
import yaml
import docx
import json

from .prepare_file_path import prepare_file_path
from .compile_flight_report import compile_flight_report


def create_flight_report(
    path_to_data_file:str,
    path_to_report_file:str,
    path_to_processing_params:str,
    path_to_flight_params:str,
    path_to_feature_names_dict:str,
    path_to_frange_names_dict:str,
    path_to_flight_report_template:str,
    **kwargs,
):

    try:
    
        # Загрузка данных в формате parquet
        data = pd.read_parquet(path=path_to_data_file, engine='pyarrow')   
        # Загрузка словаря параметро в формате yaml
        processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))
        # Загузка словаря параметров полета
        flight_params = yaml.safe_load(open(path_to_flight_params, encoding='utf-8'))
        # Загрузка словаря наименований признаков и единиц измерения
        feature_names_dict = yaml.safe_load(open(path_to_feature_names_dict, encoding='utf-8'))
        # Загрузка словаря наименований диапазонов полета
        frange_names_dict = yaml.safe_load(open(path_to_frange_names_dict, encoding='utf-8'))
        # Создание документа word по шаблону
        document = docx.Document(path_to_flight_report_template)

        # Обновление пользовательских параметров
        custom_options = kwargs.get('custom_options')
        if not (custom_options is None or custom_options == '' or custom_options == 'None'):
            custom_options = json.loads(custom_options)
            for key, value in custom_options.items():
                processing_params[key]=value

        # Создание отчета по полету
        document = compile_flight_report(
            document=document,
            data=data,
            frange_names_dict=frange_names_dict,
            feature_names_dict=feature_names_dict,
            flight_params=flight_params,
            params=processing_params,
            dpi=100,
        )

        # Подготовка директории для сохранения файла
        file_path_status = prepare_file_path(path_to_file=path_to_report_file)

        # Сохранение документа        
        document.save(path_to_report_file)
        
        # Статус обработки
        processing_status = True
    
    except Exception:
        processing_status = False
    
    return processing_status
