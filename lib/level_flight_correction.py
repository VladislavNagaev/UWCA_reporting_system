from pandas import DataFrame
from typing import List

from .allocate_ranges import maximum_duration_range


def donor_to_recipient(
    donor_data:DataFrame,
    recipient_data:DataFrame,
    correctable_features:List[str],
) -> DataFrame:
    """
    donor_data - Массив данных донора
    recipient_data - Массив данных реципиента
    """

    # Массив признаков, которые могут быть наследованы
    features = list(set(correctable_features)&set(donor_data.columns))
    # Массив признаков-донаров
    donor_features = donor_data[features].mean(axis=0).to_dict()   

    # Массив признаков, которые могут быть заимствованы
    features = list(set(donor_features.keys())&set(recipient_data.columns))   

    # Проход во признакам
    for feature in features:

        # Корректировка значения
        recipient_data[feature] = recipient_data[feature] - donor_features.get(feature)

    return recipient_data


def level_flight_correction(data:DataFrame, params:dict) -> DataFrame:

    time = params.get('time')
    range_ff = params.get('range_ff')
    correctable_features = params.get('correctable_features')

    # Выделение участка горизонтального полета максимальной длительности
    _, range_data_ff, _ = maximum_duration_range(
        data=data, time=time, frange=range_ff
    )

    # Корректировка данных реципиента по данным донора
    data = donor_to_recipient(
        donor_data=range_data_ff,
        recipient_data=data,   
        correctable_features=correctable_features,
    )
                    
    return data


