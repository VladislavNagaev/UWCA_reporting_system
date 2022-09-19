from matplotlib import pyplot
from typing import Dict, List


# Функция вывода графиков для определения диапазонов полета
def frange_plot(
    data,
    features_list:List,
    feature_names:Dict,
    frange_names:Dict,
    takeoff_range:List=(None, None), 
    takeoff_taxiings:List=[(None, None)],
    takeoff_runs:List=[(None, None)],
    forward_flights:List=[(None, None)],
    landing_runs:List=[(None, None)],
    landing_taxiings:List=[(None, None)],
    taxiings:List=[(None, None)],
    hubbles=[(None, None)],
    xlim=(None, None), 
    dpi=100
):
    
    # Turn the interactive mode off
    pyplot.ioff()  

    # Число графиков для отображения
    features_len = len(features_list)
    
    # Создание фигуры и осей
    figure, ax = pyplot.subplots(nrows=features_len, ncols=1, figsize=(16,6*features_len), dpi=dpi)

    # Построение графиков
    for k, features in enumerate(features_list):
        for feature in list(set(features) & set(data.columns)):
            if feature_names.get(feature) is not None:
                feature_name, feature_unit = feature_names.get(feature)
                if feature_unit == '' or feature_unit is None:
                    label = feature_name
                else:
                    label = feature_name + ', ' + feature_unit
            else:
                label = feature
            ax[k].plot(data['Time'].values, data[feature].values, label=label)

    # Сохранение размеров осей  
    ax_shape = dict()
    for k in range(features_len):
        ax_shape[k] = ax[k].axis()

    # Установка горизонтальных осей (границ взлетной скорости)
    if takeoff_range[0] is not None and takeoff_range[-1] is not None:
        ax[0].axhline(takeoff_range[0], color='y', linestyle='dashed', linewidth=1)
        ax[0].axhline(takeoff_range[-1], color='y', linestyle='dashed', linewidth=1)


    for franges, frange_name, color, facecolor in [
        # Диапазон рулежки перед взлетом
        (takeoff_taxiings,'takeoff_taxiing','darkgreen','green'), 
        # Диапазон разбега при взлёте
        (takeoff_runs,'takeoff_run','darkgreen','darkgreen'), 
        # Диапазон горизонтального полета
        (forward_flights,'forward_flight','red','red'), 
        # Диапазон пробега после посадки
        (landing_runs,'landing_run','darkblue','darkblue'), 
        # Диапазон рулежки после посадки
        (landing_taxiings,'landing_taxiing','darkblue','blue'), 
        # Диапазон рулежки
        (taxiings,'taxiing','yellow','yellow'),
        # Пробежка через кочку
        (hubbles, 'hubble','darkgray','gray'),
    ]:

        # Установка границ и области диапазона
        for i, frange in enumerate(franges):
            
            if i == 0:
                label = frange_names.get(frange_name)
            else:
                label = None
                
            if frange[0] is not None and frange[-1] is not None:

                for k in range(features_len):

                    ax[k].axvline(frange[0], color=color, linestyle='-', linewidth=0.5)
                    ax[k].axvline(frange[-1], color=color, linestyle='-', linewidth=0.5)
                    ax[k].fill_between(
                        x=frange, facecolor=facecolor, alpha = 0.1, label=label,
                        y1=(ax_shape.get(k)[2], ax_shape.get(k)[2]), y2=(ax_shape.get(k)[3], ax_shape.get(k)[3])
                    )    

    for k in range(features_len):

        # Установка первоначального размера осей
        ax[k].set_xlim(ax_shape.get(k)[:2])
        ax[k].set_ylim(ax_shape.get(k)[2:])

        # Установка наименований графиков
        # ax[0].set_title(label='Распределение горизонтальной скорости по времени')
        # ax[1].set_title(label='Распределение вертикальной скорости по времени')
        # ax[2].set_title(label='Распределение высоты по времени')

        # Установление подписей графиков
        ax[k].set_xlabel(xlabel='Время, с')
        # ax[0].set_ylabel(ylabel='Горизонтальная скорость, м/с')
        # ax[1].set_ylabel(ylabel='Вертикальная скорость, м/с')
        # ax[2].set_ylabel(ylabel='Высота, м')

        # Установка параметров сетки
        ax[k].minorticks_on()
        ax[k].grid(which='major', axis='both', color = 'gray', linewidth='0.5', linestyle='-')
        ax[k].grid(which='minor', axis='both', color = 'gray', linewidth='0.5', linestyle=':')

        # Установка легенды
        ax[k].legend(loc='upper right')

        # Установка границ
        if xlim[0] is not None and xlim[1] is not None:
            ax[k].set_xlim(xlim)
        
    pyplot.close() 

    return figure