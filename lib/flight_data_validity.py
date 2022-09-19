import pandas as pd
import yaml


def flight_data_validity(path_to_csv_file:str, path_to_processing_params:str):

    # Загрузка словаря параметров
    params = yaml.safe_load(open(path_to_processing_params))

    # Флаг валидности данных
    validity = True
    # Список сообщений об ошибках
    messages = []
    
    encoding = params.get('encoding')
    skiprows = params.get('skiprows')
    
    try:
        data = pd.read_csv(
            path_to_csv_file, 
            sep=';', 
            engine='c', 
            decimal=',', 
            encoding=encoding, 
            skiprows=skiprows,
        )
    except Exception:
        data = None
        message1 = 'Не удалось прочитать файл данных!'
        messages.append(message1)
        validity = False
    
    
    if data is not None:
        
        if not data.shape[0] > 0:
            message2 = 'Файл данных пуст!'
            messages.append(message2)
            validity = False
    
    
            # Список обязательных признаков
            mandatory_features = params.get('mandatory_features')
            # Список пропущенных обязательных признаков
            missing_mandatory_features = []
    
            for feature in mandatory_features:
                if feature not in data.columns:
                    missing_mandatory_features.append(feature)

            if len(missing_mandatory_features) >= 1:
                missing_columns_str = ", ".join([f"«{i}»" for i in missing_mandatory_features])
                message3 = f'Отсуствует один или более ОБЯЗАТЕЛЬНЫЙ столбец данных: {missing_columns_str}'
                messages.append(message3)
                validity = False
            
            
            # Список желательных признаков
            desirable_features = params.get('desirable_features')
            # Список пропущенных желательных признаков
            missing_desirable_features = []

            for feature in desirable_features:
                if feature not in data.columns:
                    missing_desirable_features.append(feature)
        
            if len(missing_desirable_features) >= 1:
                missing_columns_str = ", ".join([f"«{i}»" for i in missing_desirable_features])
                message4 = f'Отсуствует один или более желательный столбец данных: {missing_columns_str}'
                messages.append(message4)

            
    return validity, messages


