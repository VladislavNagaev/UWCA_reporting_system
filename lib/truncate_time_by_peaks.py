from pandas import DataFrame
from .get_frequency import get_frequency



def truncate_time_by_peaks(data:DataFrame, params:dict) -> float:
    
    time = params.get('time')
    horizontal_speed = params.get('horizontal_speed')
    feature_list = params.get('feature_list')
    section_time = params.get('section_time')
    time_offset = params.get('time_offset')
    
    # Определение частоты дискретизации данных
    frequency = get_frequency(data=data[time].values)

    # Определение времени окончания участка настройки параметров
    setting_params_time = data[time].iloc[0] + section_time
    # Определение времени первого участка с ненулевой горизонтальной скоростью
    first_motion_time = data.loc[data[horizontal_speed].round(0) > 0, time].iloc[0] - time_offset

    # Выделение анализируемого участка данных
    section_data = data[data[time] <= min(setting_params_time, first_motion_time)]

    # Список оциниваемых признаков
    feature_set = list(set(feature_list)&set(section_data.columns))
    feature_set_corr = [i + '_increment_n1_abs' for i in feature_set]

    # Формирование оцениваемых признаков
    section_data[feature_set_corr] = get_param_increment(data=section_data[feature_set], n=int(0.2*frequency)).abs()

    # Уточнение оцениваемых признаков
    # Массив значений отношения максимального к среднему для каждого признака
    max_mean_values = section_data[feature_set_corr].max(axis=0) / section_data[feature_set_corr].mean(axis=0)

    # Признаки, для которых отношение максимального к среднему более 20
    feature_set_corr_updated = max_mean_values[max_mean_values>20].index.to_list()

    # Выделение пороговых значений для классификации пиков (среднее значение + 50% максимального)
    peak_values = (
        section_data[feature_set_corr_updated].mean(axis=0) + 
        section_data[feature_set_corr_updated].max(axis=0) * 0.50
    )

    # Выделение значений, больших чем пороговая величина 
    peak_index = (section_data[feature_set_corr_updated] >= peak_values)

    # Индексы последних пиков для каждого столбца
    last_peak_index = peak_index[::-1].idxmax()

    # Выделение последнего индекса среди всех столбцов
    max_peak_index = last_peak_index.max()

    # Время индека
    max_peak_time = section_data.loc[max_peak_index, time]

    # Добавление офсета по времени
    max_peak_time += time_offset
    
    data = data[data[time] >= max_peak_time]
    
    return data


def get_param_increment(data, n:int=100):
    """
    Скорость роста параметра на последующих n точках
    """
    # Среднее значение на следующих frequency*n точках
    rolling = data.sort_index(ascending=False).rolling(window=n).mean()\
        .sort_index(ascending=True)
    # Приращение
    param_increment = (rolling-data).round(3)

    return param_increment

