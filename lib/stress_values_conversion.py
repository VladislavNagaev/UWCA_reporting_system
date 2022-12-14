from pandas import DataFrame
import numpy as np


def stress_values_conversion(data:DataFrame, params:dict) -> DataFrame:
    """
    Пересчитывает значения датчиков Ϭ по изестным значениям Н.
    """

    eqn21 = lambda x: x*100/(2.15*2)
    eqn22 = lambda x: x*2*100/(2.15*1.33*(2-x))
    eqn23 = lambda x: x*2*100/(2.15*(2-x))

    eqn31 = lambda x: x*200*1e5/(2.15*2*9.8065)
    eqn32 = lambda x: x*2*72*1e5/(2.15*1.33*(2-x)*9.80655)
    eqn33 = lambda x: x*2*200*1e5/(2.15*1.33*(2-x)*9.80655)

    list_1 = {

        '?_вно_1':{'?Вно.1':eqn21,'НВно1':eqn31},
        '?_вно_2':{'?Вно.2':eqn21,'НВно2':eqn31},
        '?_вно_3':{'?Вно.3':eqn21,'Нвно3':eqn31},
        '?_вно_4':{'?Вно.4':eqn21,'Нвно4':eqn31},

        '?_цно_1':{'?Цно.1':eqn21,'Нцно1':eqn31},
        '?_цно_2':{'?Цно.2':eqn21,'Нцно2':eqn31},
        '?_цно_3':{'?Цно.3':eqn21,'Нцно3':eqn31}, 

        '?_12_пр':{'?12пр.':eqn22,'Н12пр':eqn32},
        '?_12_лев':{'?12лев.':eqn22,'Н12лев':eqn32},

        '?_14_пр':{'?14пр.':eqn22,'Н14пр':eqn32},
        '?_14_лев':{'?14лев.':eqn22,'Н14лев':eqn32},

        '?_рлош_1':{'?Рлош.1':eqn21,'НРлош1':eqn31},
        '?_рлош_2':{'?Рлош.2':eqn21,'НРлош2':eqn31},
        '?_рлош_3':{'?Рлош.3':eqn21,'НРлош3':eqn31},
        '?_рлош_4':{'?Рлош.4':eqn21,'НРлош4':eqn31},
        '?_рлош_5':{'?Рлош.5':eqn21,'НРлош5':eqn31},

        '?_рпош_1':{'?Рпош.1':eqn21,'НРпош1':eqn31},
        '?_рпош_2':{'?Рпош.2':eqn21,'НРпош2':eqn31},
        '?_рпош_3':{'?Рпош.3':eqn21,'НРпош3':eqn31},
        '?_рпош_4':{'?Рпош.4':eqn21,'НРпош4':eqn31},
        '?_рпош_5':{'?Рпош.5':eqn21,'НРпош5':eqn31},

        '?_клош':{'?клош':eqn21,'НКлош':eqn31},
        '?_кпош':{'?кпош':eqn21,'НКпош':eqn31},

        '?_блош':{'?Блош.':eqn21,'НБлош':eqn31},
        '?_бпош':{'?Бпош.':eqn21,'НБпош':eqn31},

        '?_плош':{'?Плош.':eqn22,'НПлош':eqn33},
        '?_ппош':{'?Ппош.':eqn22,'НПпош':eqn33},

        '?_шш_1':{'?шш1':eqn22,'Ншш1':eqn33},
        '?_шш_2':{'?шш2':eqn21,'Ншш2':eqn31},

        '?_1':{'?1':eqn23,'Н1':eqn33},
        '?_2':{'?2':eqn21,'Н2':eqn31},
        '?_3':{'?3':eqn22,'Н3':eqn32},
        '?_4':{'?4':eqn22,'Н4':eqn32},
        '?_5':{'?5':eqn22,'Н5':eqn32},
        '?_6':{'?6':eqn22,'Н6':eqn32},  }

    eqn21inv = lambda x: x*2.15*2/100
    eqn22inv = lambda x: x*2*2.15*1.33/(2*100+2.15*1.33*x)
    eqn23inv = lambda x: x*2*2.15/(2*100+2.15*x)

    list_2 = { 

        '?Вно.1':{'?_вно_1':eqn21inv},
        '?Вно.2':{'?_вно_2':eqn21inv},
        '?Вно.3':{'?_вно_3':eqn21inv},
        '?Вно.4':{'?_вно_4':eqn21inv},

        '?Цно.1':{'?_цно_1':eqn21inv},
        '?Цно.2':{'?_цно_2':eqn21inv},
        '?Цно.3':{'?_цно_3':eqn21inv},

        '?12пр.':{'?_12_пр':eqn22inv},
        '?12лев.':{'?_12_лев':eqn22inv}, 

        '?14пр.':{'?_14_пр':eqn22inv},
        '?14лев.':{'?_14_лев':eqn22inv},

        '?Рлош.1':{'?_рлош_1':eqn21inv},
        '?Рлош.2':{'?_рлош_2':eqn21inv},
        '?Рлош.3':{'?_рлош_3':eqn21inv},
        '?Рлош.4':{'?_рлош_4':eqn21inv},
        '?Рлош.5':{'?_рлош_5':eqn21inv}, 

        '?Рпош.1':{'?_рпош_1':eqn21inv},
        '?Рпош.2':{'?_рпош_2':eqn21inv},
        '?Рпош.3':{'?_рпош_3':eqn21inv},
        '?Рпош.4':{'?_рпош_4':eqn21inv},
        '?Рпош.5':{'?_рпош_5':eqn21inv},

        '?клош':{'?_клош':eqn21inv},
        '?кпош':{'?_кпош':eqn21inv}, 

        '?Блош.':{'?_блош':eqn21inv},
        '?Бпош.':{'?_бпош':eqn21inv}, 

        '?Плош.':{'?_плош':eqn22inv},
        '?Ппош.':{'?_ппош':eqn22inv}, 

        '?шш1':{'?_шш_1':eqn22inv},
        '?шш2':{'?_шш_2':eqn21inv},

        '?1':{'?_1':eqn23inv},
        '?2':{'?_2':eqn21inv},
        '?3':{'?_3':eqn22inv},
        '?4':{'?_4':eqn22inv},
        '?5':{'?_5':eqn22inv},
        '?6':{'?_6':eqn22inv},   }

    eqn31inv = lambda x: x*2.15*2*9.8065/(200*1e5)
    eqn32inv = lambda x: x*2*2.15*1.33*9.8065/(2*72*1e5+x*2.15*1.33*9.8065)
    eqn33inv = lambda x: x*2*2.15*1.33*9.8065/(2*200*1e5+x*2.15*1.33*9.8065)

    list_3 = {

        'НВно1':{'?_вно_1':eqn31inv},
        'НВно2':{'?_вно_2':eqn31inv},
        'Нвно3':{'?_вно_3':eqn31inv},
        'Нвно4':{'?_вно_4':eqn31inv},

        'Нцно1':{'?_цно_1':eqn31inv},
        'Нцно2':{'?_цно_2':eqn31inv},
        'Нцно3':{'?_цно_3':eqn31inv},

        'Н12пр':{'?_12_пр':eqn32inv},
        'Н12лев':{'?_12_лев':eqn32inv},

        'Н14пр':{'?_14_пр':eqn32inv},
        'Н14лев':{'?_14_лев':eqn32inv},

        'НРлош1':{'?_рлош_1':eqn31inv},
        'НРлош2':{'?_рлош_2':eqn31inv},
        'НРлош3':{'?_рлош_3':eqn31inv},
        'НРлош4':{'?_рлош_4':eqn31inv},
        'НРлош5':{'?_рлош_5':eqn31inv},

        'НРпош1':{'?_рпош_1':eqn31inv},
        'НРпош2':{'?_рпош_2':eqn31inv},
        'НРпош3':{'?_рпош_3':eqn31inv},
        'НРпош4':{'?_рпош_4':eqn31inv},
        'НРпош5':{'?_рпош_5':eqn31inv},

        'НКлош':{'?_клош':eqn31inv},
        'НКпош':{'?_кпош':eqn31inv},

        'НБлош':{'?_блош':eqn31inv},
        'НБпош':{'?_бпош':eqn31inv},

        'НПлош':{'?_плош':eqn33inv},
        'НПпош':{'?_ппош':eqn33inv},

        'Ншш1':{'?_шш_1':eqn33inv},
        'Ншш2':{'?_шш_2':eqn31inv},

        'Н1':{'?_1':eqn33inv},
        'Н2':{'?_2':eqn31inv},
        'Н3':{'?_3':eqn32inv},
        'Н4':{'?_4':eqn32inv},
        'Н5':{'?_5':eqn32inv},
        'Н6':{'?_6':eqn32inv},    }    
    
        
    # Пересчитаем значения датчиков Ϭ (list_1) по изестным значениям Н (list_3).

    # Текущие наименования столбцов
    column_names = data.columns
    # Множество параметров для изменения
    features = list(set(list_3.keys()) & set(column_names))

    for feature in features:

        # Cписок изменяемых величин
        variable_features = np.array(list(list_3.get(feature).keys()))
        # Список индексов, соответствующих условию
        variable_feature_indexes = [x[0]=='?' for x in variable_features]
        # Изменяемая величина
        variable_feature = variable_features[variable_feature_indexes][0]

        # Корректировка значений в исходном массиве
        data[variable_feature] = data[feature].apply(list_3.get(feature).get(variable_feature))



    # Пересчитаем значения Ϭ (list_2) по уже найденным значеням датчиков Ϭ (list_1).

    # Текущие наименования столбцов
    column_names = data.columns
    # Множество параметров для изменения
    features = list(set(list_1.keys()) & set(column_names))

    for feature in features:

        # Cписок изменяемых величин
        variable_features = np.array(list(list_1.get(feature).keys()))
        # Список индексов, соответствующих условию
        variable_feature_indexes = [x[0]=='?' for x in variable_features]
        # Изменяемая величина
        variable_feature = variable_features[variable_feature_indexes][0]

        # Корректировка значений в исходном массиве
        data[variable_feature] = data[feature].apply(list_1.get(feature).get(variable_feature))
    
    return data

