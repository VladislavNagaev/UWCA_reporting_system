import yaml
import docx

from typing import List

from .prepare_file_path import prepare_file_path
from .compile_flight_consolidated_data import compile_flight_consolidated_data
from .compile_summary_flight_report import compile_summary_flight_report
import logging

def create_summary_flight_report(**kwargs):
    
    """
    Построение сводного отчета для списка файлов path_to_data_file_list
    """


    ti = kwargs.get('ti')

    path_to_data_file_list = ti.xcom_pull(key='path_to_data_file_list')
    path_to_report_file = ti.xcom_pull(key='path_to_report_file')
    path_to_processing_params = ti.xcom_pull(key='path_to_processing_params')
    path_to_processing_addition_params = ti.xcom_pull(key='path_to_processing_addition_params')
    path_to_feature_names_dict = ti.xcom_pull(key='path_to_feature_names_dict')
    path_to_frange_names_dict = ti.xcom_pull(key='path_to_frange_names_dict')
    path_to_flight_report_template = ti.xcom_pull(key='path_to_flight_report_template')


    try:

        # Загрузка словаря параметро в формате yaml
        processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))
        # Загрузка словаря параметро в формате yaml
        processing_addition_params = yaml.safe_load(open(path_to_processing_addition_params, encoding='utf-8'))
        # Загрузка словаря наименований признаков и единиц измерения
        feature_names_dict = yaml.safe_load(open(path_to_feature_names_dict, encoding='utf-8'))
        # Загрузка словаря наименований диапазонов полета
        frange_names_dict = yaml.safe_load(open(path_to_frange_names_dict, encoding='utf-8'))
        # Создание документа word по шаблону
        document = docx.Document(path_to_flight_report_template)

        
        # Подготовка сводной таблицы данных
        consolidated_data = compile_flight_consolidated_data(
            path_to_data_file_list=path_to_data_file_list, 
            params=processing_params,
        )
        
        # Подготовка сводной таблицы данных
        consolidated_addition_data = compile_flight_consolidated_data(
            path_to_data_file_list=path_to_data_file_list, 
            params=processing_addition_params,
        )
        
        # Создание отчета по сводной таблице
        document = compile_summary_flight_report(
            document=document,
            consolidated_data=consolidated_data,  
            consolidated_addition_data=consolidated_addition_data,
            feature_names_dict=feature_names_dict,
            frange_names_dict=frange_names_dict,
            params=processing_params,
            addition_params=processing_addition_params,
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