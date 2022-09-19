from pandas import DataFrame
import numpy as np


def strain_gauge_loads(data:DataFrame, params:dict) -> DataFrame:

    # Параметр сглаживания
    smoothing = params.get('smoothing')

    if 'НВно1' in data.columns:
        
        # Нагрузка на ПОШ по оси Y по результатам тензометрии
        P_front_wheel_Y_tensometry = 1.9 * data['НВно1']
        data['P_front_wheel_Y_tensometry'] = P_front_wheel_Y_tensometry

    if ('P_front_wheel_Y_tensometry' in data.columns) and ('Нцно2' in data.columns):

        k_fron_wheel = np.mean(-1.49 * data['Нцно2'][:smoothing] / data['P_front_wheel_Y_tensometry'][:smoothing])

        # Нагрузка на ПОШ по оси X по результатам тензометрии
        P_front_wheel_X_tensometry = (1.49 * data['Нцно2'] + k_fron_wheel * data['P_front_wheel_Y_tensometry'])
        data['P_front_wheel_X_tensometry'] = P_front_wheel_X_tensometry

    if 'Нцно1' in data.columns:    

        # Нагрузка на ПОШ по оси Z по результатам тензометрии
        P_front_wheel_Z_tensometry = 0.97 * data['Нцно1']
        data['P_front_wheel_Z_tensometry'] = P_front_wheel_Z_tensometry

    if 'НРлош3' in data.columns:

        # Нагрузка на левую ООШ по оси Y по результатам тензометрии
        P_main_wheel_left_Y_tensometry = 2.1 * data['НРлош3']
        data['P_main_wheel_left_Y_tensometry'] = P_main_wheel_left_Y_tensometry

    if ('НРлош4' in data.columns) and ('P_main_wheel_left_Y_tensometry' in data.columns):

        k_main_wheel_left = np.mean(-1.21 * data['НРлош4'][:smoothing] / data['P_main_wheel_left_Y_tensometry'][:smoothing])

        # Нагрузка на левую ООШ по оси X по результатам тензометрии
        P_main_wheel_left_X_tensometry = (1.21 * data['НРлош4'] + k_main_wheel_left * data['P_main_wheel_left_Y_tensometry'])
        data['P_main_wheel_left_X_tensometry'] = P_main_wheel_left_X_tensometry

    if ('НПлош' in data.columns) and ('P_main_wheel_left_Y_tensometry' in data.columns): 

        # Нагрузка на левую ООШ по оси Z по результатам тензометрии
        P_main_wheel_left_Z_tensometry = 3.02 * data['НПлош'] / data['P_main_wheel_left_Y_tensometry']
        data['P_main_wheel_left_Z_tensometry'] = P_main_wheel_left_Z_tensometry            

    if 'НРпош3' in data.columns:

        # Нагрузка на правую ООШ по оси Y по результатам тензометрии
        P_main_wheel_right_Y_tensometry = 2.1 * data['НРпош3']
        data['P_main_wheel_right_Y_tensometry'] = P_main_wheel_right_Y_tensometry

    if ('НРпош4' in data.columns) and ('P_main_wheel_right_Y_tensometry' in data.columns):

        k_main_wheel_right = np.mean(-1.21 * data['НРпош4'][:smoothing] / data['P_main_wheel_right_Y_tensometry'][:smoothing])

        # Нагрузка на правую ООШ по оси X по результатам тензометрии
        P_main_wheel_right_X_tensometry = (1.21 * data['НРпош4'] + k_main_wheel_right * data['P_main_wheel_right_Y_tensometry'])
        data['P_main_wheel_right_X_tensometry'] = P_main_wheel_right_X_tensometry

    if ('НПпош' in data.columns) and ('P_main_wheel_right_Y_tensometry' in data.columns): 

        # Нагрузка на правую ООШ по оси Z по результатам тензометрии
        P_main_wheel_right_Z_tensometry = 3.02 * data['НПпош'] / data['P_main_wheel_right_Y_tensometry']
        data['P_main_wheel_right_Z_tensometry'] = P_main_wheel_right_Z_tensometry                 

    return data

