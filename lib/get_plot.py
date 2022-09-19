import matplotlib
from matplotlib import pyplot
matplotlib.style.use('ggplot')
from typing import Dict


def get_plot(
    data,
    franges_data_dict:Dict,
    franges_dict:Dict,
    frange_names_dict:Dict,
    feature_names_dict:Dict,
    feature1,
    feature2=None,
    time_feature='Time',
    max_value=False,
    fill_between=True,
    dpi=100
):
    """
    Функция обработки графиков
    """
    
    # Turn the interactive mode off
    pyplot.ioff()
    
    # Создание фигуры и осей
    figure, ax = pyplot.subplots(figsize=(16,6), dpi=dpi)

    # Построение графиков
    ax.plot(data[time_feature].values, data[feature1].values, label=feature1)
    if feature2:
        ax.plot(data[time_feature].values, data[feature2].values, label=feature2)

    # Сохранение размеров осей    
    ax_shape = ax.axis()

    frange_params_default = {
        'takeoff_taxiings': {'color': 'darkgreen', 'facecolor': 'green', 'cycle_numbers': None}, 
        'takeoff_runs': {'color': 'darkgreen', 'facecolor': 'darkgreen', 'cycle_numbers': None},
        'forward_flights': {'color': 'red', 'facecolor': 'red', 'cycle_numbers': None},
        'landing_runs': {'color': 'darkblue', 'facecolor': 'darkblue', 'cycle_numbers': None},
        'landing_taxiings': {'color': 'darkblue', 'facecolor': 'blue', 'cycle_numbers': None},
        'taxiings': {'color': 'yellow', 'facecolor': 'yellow', 'cycle_numbers': None},
        'hubbles': {'color': 'darkgray', 'facecolor': 'gray', 'cycle_numbers': None},   
    }
    
    if franges_dict == 'full':
        franges_dict = frange_params_default
    elif isinstance(franges_dict, str):
        try:
            franges_dict = {franges_dict: frange_params_default.get(franges_dict)}
        except Exception:
            raise
    elif isinstance(franges_dict, list):
        franges_dict = {frange_dict: frange_params_default.get(frange_dict) for frange_dict in franges_dict}

    
    for frange_name in franges_dict.keys():

        frange_params = franges_dict.get(frange_name)

        color = frange_params.get('color')
        if color is None:
            try:
                color = frange_params_default.get('frange_name').get('color')
            except Exception:
                color = 'red'

        facecolor  = frange_params.get('facecolor')
        if facecolor is None:
            try:
                facecolor = frange_params_default.get('frange_name').get('facecolor')
            except Exception:
                facecolor = 'red'

        frange_data_dict = franges_data_dict.get(frange_name)

        if frange_data_dict is not None:

            cycle_numbers = frange_params.get('cycle_numbers')
            if cycle_numbers is None:
                cycle_numbers = frange_data_dict.keys()

            for i, cycle_number in enumerate(cycle_numbers):
                cycle_data = frange_data_dict.get(cycle_number)

                if cycle_data is not None:

                    left_boundary = cycle_data.loc[cycle_data.index[0], time_feature]
                    right_boundary = cycle_data.loc[cycle_data.index[-1], time_feature]

                    label = frange_names_dict.get(frange_name) if i == 0 else None

                    ax.axvline(left_boundary, color=color, linestyle='-', linewidth=0.5)
                    ax.axvline(right_boundary, color=color, linestyle='-', linewidth=0.5)

                    if fill_between:

                        ax.fill_between(
                            x=(left_boundary, right_boundary),
                            y1=(ax_shape[2], ax_shape[2]), 
                            y2=(ax_shape[3], ax_shape[3]),
                            facecolor=facecolor, 
                            alpha = 0.1, 
                            label=label
                        )   

    # Установка первоначального размера осей
    ax.set_xlim(ax_shape[:2])
    ax.set_ylim(ax_shape[2:])  

    # Установка наименований графиков
    ax.set_title(label=f'Распределение параметра "{feature_names_dict[feature1][0]}"')

    # Установление подписей графиков
    ax.set_xlabel(xlabel='Время' + ', ' + 'с')

    # Установка параметров сетки
    ax.minorticks_on()
    ax.grid(which='major', axis='both', color='gray', linewidth='0.5', linestyle='-')
    ax.grid(which='minor', axis='both', color='gray', linewidth='0.5', linestyle=':')


    if max_value:

        s1 = f'{feature1} max {round(data[feature1].max(), 1)}\n{feature1} min {round(data[feature1].min(), 1)}'
        ax.text(0.02*(ax_shape[1]-ax_shape[0])+ax_shape[0], 
                (0.9*(ax_shape[3] - ax_shape[2]))+ax_shape[2], 
                s1, bbox=dict(facecolor='red', alpha=0.5))

        if feature2:
            s2 = f'{feature2} max {round(data[feature2].max(), 1)}\n{feature2} min {round(data[feature2].min(), 1)}'
            ax.text(0.02*(ax_shape[1]-ax_shape[0])+ax_shape[0], 
                    (0.8*(ax_shape[3] - ax_shape[2]))+ax_shape[2], 
                    s2, bbox=dict(facecolor='blue', alpha=0.5))


    # Установка легенды       
    ax.legend(loc='upper right')
    
    # Закрытие графика
    pyplot.close() 
    
    return figure    

