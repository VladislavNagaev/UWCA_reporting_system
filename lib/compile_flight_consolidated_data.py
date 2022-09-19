import os
import pandas as pd
from pandas import DataFrame
import numpy as np
import os

from typing import List, Union

from .allocate_ranges import allocate_ranges


# Функция возвращает таблицу экстремумов для одного полета
def get_flight_consolidated_data(
    tables_params,
    franges_name_list,
    franges_data_dict,
    file_name,
):

    # Создание сводной таблицы
    consolidated_data = pd.DataFrame()

    for table_params in tables_params:
        
        table_name = table_params.get('table_name')
        feature_for_table = table_params.get('feature_for_table')
        tracked_features = table_params.get('tracked_features')

        for franges_name in franges_name_list:

            # Список участков диапазона
            franges = franges_data_dict.get(franges_name)

            # Создание пустого массива для суммирования данных по участкам диапазона
            full_frange_data = pd.DataFrame()

            if franges is not None:
                for frange_number, frange_data in franges.items():
                    # Объединение данных по всем участкам диапазона
                    full_frange_data = pd.concat([full_frange_data, frange_data])

            # Список наименований строк
            row_names = []
            # Списое состояний экстремумов
            extremum_names = []

            # Создание пустого массива результирующей таблицы
            extremum_table = pd.DataFrame()

            # Получение данных для таблицы
            for tracked_feature in tracked_features:

                if tracked_feature in full_frange_data.columns:

                    # Добавление признака в список наимнований строк
                    row_names.append(tracked_feature)
                    row_names.append(tracked_feature)

                    # Добавление состояния экстремума в список
                    extremum_names.append('max')
                    extremum_names.append('min')

                    # Индексы максимального и минимального значения отслеживаемого признака
                    idxmax = full_frange_data[tracked_feature].idxmax()
                    idxmin = full_frange_data[tracked_feature].idxmin()

                    # Признаки для таблицы с учетом столбцов в массиве
                    feature_for_table_corr = [
                        feature_for_table_i
                        for feature_for_table_i in feature_for_table
                        if feature_for_table_i in full_frange_data.columns 
                    ]

                    # Заполнение результирующей таблицы
                    extremum_table = pd.concat([
                        extremum_table,
                        full_frange_data[feature_for_table_corr].loc[[idxmax]],
                        full_frange_data[feature_for_table_corr].loc[[idxmin]],
                    ])  

            # Добавление в результирующую таблицу наименования диапазона
            extremum_table['frange_name'] = franges_name
            # Добавление в результирующую таблицу наименований строк
            extremum_table['row_name'] = row_names                        
            # Добавление в результирующую таблицу состояний экстремумов
            extremum_table['extremum'] = extremum_names

            extremum_table.index=list(range(consolidated_data.shape[0],consolidated_data.shape[0]+extremum_table.shape[0]))

            # Добавление результирующей таблицы в сводную таблицу
            consolidated_data = pd.concat([consolidated_data, extremum_table])
    

    # Добавление в результирующую таблицу наименования файла
    consolidated_data['file_name'] = file_name

    return consolidated_data  



# Функция сортирует данные, выводит экстремумы по всем полетам
def sort_consolidated_data(consolidated_data):

    sort_consolidated_data = pd.DataFrame()

    # Списое наименований диапазонов полета
    frange_name_list = consolidated_data['frange_name'].unique()

    # Проход по наименованиям диапазонов полета
    for frange_name in frange_name_list:

        # Список наименований строк
        row_name_list = consolidated_data[
            (consolidated_data['frange_name']==frange_name)
        ]['row_name'].unique()

        # Проход по наименованиям строк
        for row_name in row_name_list:

            # Результирующая таблица максимумов
            max_extremum_table = consolidated_data[
                (consolidated_data['frange_name']==frange_name) & 
                (consolidated_data['row_name']==row_name) & 
                (consolidated_data['extremum']=='max')
            ]

            # Результирующая таблица минимумов
            min_extremum_table = consolidated_data[
                (consolidated_data['frange_name']==frange_name) & 
                (consolidated_data['row_name']==row_name) & 
                (consolidated_data['extremum']=='min')
            ]  

            # Индексы максимального и минимального значения отслеживаемого признака
            idxmax = max_extremum_table[row_name].idxmax()
            idxmin = min_extremum_table[row_name].idxmin()

            # Отсортированные таблицы экстремумов
            sort_max_extremum_table = max_extremum_table.loc[[idxmax]]
            sort_min_extremum_table = min_extremum_table.loc[[idxmin]]
            
            # Список наименований файлов, по которым выполнялась сортировка
            file_name_list = max_extremum_table['file_name'].unique()
            
            # Добавление списка наименований файлов, по которым выполнялась сортировка
            sort_max_extremum_table['file_name_list'] = ', '.join(list(file_name_list))    
            sort_min_extremum_table['file_name_list'] = ', '.join(list(file_name_list))                
            
            # Добавление экстремумов в отсортированную таблицу
            sort_consolidated_data = pd.concat([
                sort_consolidated_data, 
                sort_max_extremum_table,
                sort_min_extremum_table,
            ])

    return sort_consolidated_data      


def compile_flight_consolidated_data(
    path_to_data_file_list:List[str], 
    params,
):
    
    tables_params = params.get('tables_params')
    franges_name_list = params.get('franges_name_list')
    
    # Создание сводной таблицы
    full_consolidated_data = pd.DataFrame()

    # Проход по списку
    for path_to_data_file in path_to_data_file_list:
        
        path_to_file = path_to_data_file

        file = os.path.basename(path_to_file)
        file_name = ''.join(os.path.splitext(file)[:-1])
        file_format = ''.join(os.path.splitext(file)[-1:])[1:]
        file_base_path = os.path.dirname(path_to_file)

        # Загрузка данных в формате parquet
        data = pd.read_parquet(path=path_to_file, engine='pyarrow') 

        # Выделение диапазонов полета
        franges_data_dict = allocate_ranges(data=data)

        # Удаление данных по участкам горизонтального полета
        forward_flights = franges_data_dict.get('forward_flights')
        for frange_number, frange_data in forward_flights.items():
            data.loc[frange_data.index] = np.nan    
        # Удаление данных по участкам горизонтального полета для всего диапазона эксперимента
        franges_data_dict['full']['full'] = data
        
        consolidated_data = get_flight_consolidated_data(
            tables_params=tables_params,
            franges_name_list=franges_name_list,
            franges_data_dict=franges_data_dict,
            file_name=file_name,
        ) 

        if consolidated_data is not None:

            consolidated_data.index=list(range(
                full_consolidated_data.shape[0],
                full_consolidated_data.shape[0]+consolidated_data.shape[0]
            ))
            full_consolidated_data = pd.concat([full_consolidated_data, consolidated_data])
    
    # Сортировка сводной таблицы
    sorted_consolidated_data = sort_consolidated_data(full_consolidated_data)
    
    return sorted_consolidated_data




