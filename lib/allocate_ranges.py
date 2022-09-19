from pandas import DataFrame


def allocate_ranges(
    data:DataFrame, 
    frange:str='frange', 
    boundaries_only:bool=False, 
    **kwargs,
) -> dict:
    """
    Возвращает словарь данных по диапазонам полета
    """
    
    if not isinstance(data, DataFrame):
        raise TypeError(
            '<return_type> should be a pandas.DataFrame.'
        )
    
    if not frange in data.columns:
        raise ValueError(
            f'Received frame <data> does not contain a <{frange}> column.'
        )    
    
    # Пустой словарь данных по диапазонам полета
    franges_data_dict = dict()

    # Проход по группам данных диапазонов полета
    for frange_column, frange_data in data.groupby(by=frange):  
        if (frange_column != 0) and (frange_column != ''):

            # Наименование и номер диапазона полета
            frange_name, frange_number = frange_column.split('-')
            frange_number = int(frange_number)

            # Наименование группы диапазонов
            franges_name = frange_name + 's'

            # Получение словаря данных по конкретной группе диапазонов полета
            franges_data = franges_data_dict.get(franges_name)

            if franges_data is None:
                franges_data = {frange_number:frange_data}
            else:
                franges_data[frange_number] = frange_data

            # Обновление словаря данных по диапазонам полета
            franges_data_dict[franges_name] = franges_data
    
    franges_data_dict['full'] = {'full':data}


    if boundaries_only is True:
        for franges_name in franges_data_dict.keys():
            franges_data = franges_data_dict.get(franges_name)
            frange_boundaries = [tuple(franges_data.get(frange_name).iloc[[0,-1]].loc[:,'Time'].values) for frange_name in franges_data.keys()]
            franges_data_dict[franges_name] = frange_boundaries
    
    
    return franges_data_dict


def maximum_duration_range(data:DataFrame, time:str='Time', frange:str='forward_flights'):
    
    # Словарь данных по диапазонам полета
    franges_data_dict = allocate_ranges(data)
    # Cловарь данных по конкретной группе диапазонов полета
    franges_data = franges_data_dict.get(frange)


    # Присвоение номера максимального полета
    frange_number_max = None
    # Присвоение данных максимального полета
    frange_data_max = None
    # Определение длительности максимального полета
    frange_duration_max = 0

    if franges_data is not None:

        # Проход по диапазонам полета конкретной группы
        for frange_number, frange_data in franges_data.items():

            # Определение длительности полета
            frange_duration = frange_data[time].max() - frange_data[time].min()

            # Сравнение длительности полета 
            if frange_duration > frange_duration_max:

                # Обновление номера максимального полета
                frange_number_max = frange_number
                # Обновление данных максимального полета
                frange_data_max = frange_data
                # Обновление длительности максимального полета
                frange_duration_max = frange_duration

    return frange_number_max, frange_data_max, frange_duration_max

