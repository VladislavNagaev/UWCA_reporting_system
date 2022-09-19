from matplotlib import pyplot
from typing import Dict, List
from pandas import DataFrame


def control_plot(
    data_result:DataFrame,
    feature_list:List,
    feature_names:Dict,
    frange_names:Dict,
    data_source:DataFrame=None,
    takeoff_taxiings:List=[(None, None)],
    takeoff_runs:List=[(None, None)],
    forward_flights:List=[(None, None)],
    landing_runs:List=[(None, None)],
    landing_taxiings:List=[(None, None)],
    taxiings:List=[(None, None)],
    hubbles:List=[(None, None)],
    dpi:int=100,
    **kwargs,
):
    
    # Список признаков
    features = [feature for feature in feature_list if feature in list(set(feature_list) & set(data_result.columns))]

    # Число графиков для отображения
    feature_len = len(features)
    

    # Turn the interactive mode off
    pyplot.ioff()  

    figure_list = list()

    # Проход по признакам
    for feature in features:

        # Создание фигуры и осей
        figure, ax = pyplot.subplots(nrows=1, ncols=1, figsize=(16,6), dpi=dpi)

        for data, label, color in zip((data_source, data_result), ('До обработки', 'После обработки'), ('blue', 'red'),):

            if data is not None:

                # Построение графика
                ax.plot(data['Time'].values, data[feature].values, color=color, label=label, )

                # Установка наименования графика
                if feature_names.get(feature) is not None:
                    feature_name, feature_unit = feature_names.get(feature)
                    if feature_unit == '' or feature_unit is None:
                        title = feature_name
                    else:
                        title = feature_name + ', ' + feature_unit
                else:
                    title = feature
                ax.set_title(label=title)

        # Сохранение размеров осей  
        ax_shape = ax.axis()

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

            if franges is not None:

                # Установка границ и области диапазона
                for i, frange in enumerate(franges):
                    
                    if i == 0:
                        label = frange_names.get(frange_name)
                    else:
                        label = None
                        
                    if frange[0] is not None and frange[-1] is not None:

                        ax.axvline(frange[0], color=color, linestyle='-', linewidth=0.5)
                        ax.axvline(frange[-1], color=color, linestyle='-', linewidth=0.5)
                        ax.fill_between(
                            x=frange, facecolor=facecolor, alpha = 0.1, label=label,
                            y1=(ax_shape[2], ax_shape[2]), y2=(ax_shape[3], ax_shape[3])
                        )    

        # Установка первоначального размера осей
        ax.set_xlim(ax_shape[:2])
        ax.set_ylim(ax_shape[2:])

        # Установление подписей графиков
        ax.set_xlabel(xlabel='Время, с')

        # Установка параметров сетки
        ax.minorticks_on()
        ax.grid(which='major', axis='both', color = 'gray', linewidth='0.5', linestyle='-')
        ax.grid(which='minor', axis='both', color = 'gray', linewidth='0.5', linestyle=':')

        # Установка легенды
        ax.legend(loc='upper right')

        pyplot.close() 

        figure_list.append(figure)

    return figure_list