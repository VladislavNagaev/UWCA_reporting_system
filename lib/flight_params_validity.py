import yaml
import datetime


def flight_params_validity(path_to_yaml_file:str, path_to_flight_params_requirements:str):

    # Загрузка словаря требований
    flight_params_requirements = yaml.safe_load(open(path_to_flight_params_requirements))

    # Флаг валидности данных
    validity = True
    # Список сообщений об ошибках
    messages = []

    try:
        yaml_file = yaml.safe_load(open(path_to_yaml_file))
    except Exception:
        yaml_file = None
        message1 = 'Не удалось прочитать файл данных!'
        messages.append(message1)
        validity = False

    if yaml_file is not None:
        
        if not len(yaml_file) > 0:
            message2 = 'Файл данных пуст!'
            messages.append(message2)
            validity = False
        
        # Список обязательных признаков
        mandatory_features = flight_params_requirements.get('mandatory_features')
        # Список пропущенных обязательных признаков
        missing_mandatory_features = []      

        for feature in mandatory_features:
            if feature not in yaml_file.keys():
                missing_mandatory_features.append(feature)
    
        if len(missing_mandatory_features) >= 1:
            missing_columns_str = ", ".join([f"«{i}»" for i in missing_mandatory_features])
            message3 = f'Отсуствует один или более ОБЯЗАТЕЛЬНЫЙ параметр: {missing_columns_str}'
            messages.append(message3)
            validity = False
        
        
        # Список желательных признаков
        desirable_features = flight_params_requirements.get('desirable_features')
        # Список пропущенных желательных признаков
        missing_desirable_features = []

        for feature in desirable_features:
            if feature not in yaml_file.keys():
                missing_desirable_features.append(feature)
    
        if len(missing_desirable_features) >= 1:
            missing_columns_str = ", ".join([f"«{i}»" for i in missing_desirable_features])
            message4 = f'Отсуствует один или более желательный параметр: {missing_columns_str}'
            messages.append(message4)      


        for feature, value in yaml_file.items():
            x = value
            feature_requirements = flight_params_requirements.get(feature)
            if feature_requirements is not None and isinstance(feature_requirements, list):
                for feature_requirement in feature_requirements:
                    try:
                        eval(feature_requirement)
                    except:
                        message = f'Параметр «{feature}» не соответствует следующему условию: «{feature_requirement}»!'
                        messages.append(message)


    return validity, messages