from pandas import DataFrame
import numpy as np


def simple_data_correction(data:DataFrame, params:dict) -> DataFrame: 
    
    removed_startswith_columns = params.get('removed_startswith_columns')
    n_list = params.get('n_list')
    c_list = params.get('c_list')
    v_list = params.get('v_list')
    iv_list = params.get('iv_list')
    h_list = params.get('h_list')
    error_threshold = params.get('error_threshold')
    ny_list = params.get('ny_list')

    # Удаление столбцов, начинабщихя с ...
    data = data.iloc[:, [all([not column.startswith(feature) for feature in removed_startswith_columns]) for column in data.columns]]

    # Перевод перегрузки в безрамерную величину
    data[list(set(n_list)&set(data.columns))] = data[list(set(n_list)&set(data.columns))].apply(lambda x: x/9.81)    

    # Указание единиц измерения
    for feature1, feature2 in c_list:
        if feature1 in data.columns:
            data[feature2] = data[feature1]
            data = data.drop(columns=feature1, inplace=False)
        
    # Перевод скоростей из км/ч в м/с
    for feature1, feature2 in v_list:
        if feature1 in data.columns:
            data[feature2] = data[feature1].apply(lambda x: x/3.6)

    # Перевод скоростей из м/с в км/ч
    for feature1, feature2 in iv_list:
        if feature1 in data.columns:
            data[feature2] = data[feature1].apply(lambda x: x*3.6)
    
    # Удаление ошибок радовысотомера
    for feature in h_list:
        if feature in data.columns:
            array = data[feature].values
            diff_array = array - np.roll(array, shift=1)
            diff_index = np.where(np.abs(diff_array) >= error_threshold)[0]
            for error_start, error_end in zip(diff_index[0::2], diff_index[1::2]):
                value = np.mean([data[feature].iloc[error_start-1], data[feature].iloc[error_end]])
                data[feature].loc[error_start:error_end-1] =\
                    data[feature].loc[error_start:error_end-1].apply(lambda x: value)                        

    # Приращение перегрузки к единице
    for feature in ny_list:
        if feature in data.columns:
            new_feature = str(feature) + str('-1')
            data[new_feature] = data[feature] - 1
    
    return data


