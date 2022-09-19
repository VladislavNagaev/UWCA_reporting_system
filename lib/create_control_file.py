import pandas as pd
import yaml

from .control_plot import control_plot
from .prepare_file_path import prepare_file_path
from .allocate_ranges import allocate_ranges
from .save_figures_to_file import save_figures_to_file


def create_control_file(
    path_to_result_file:str, 
    path_to_control_file:str,
    path_to_processing_params:str, 
    path_to_feature_names_dict:str,
    path_to_frange_names_dict:str,
    path_to_source_file:str=None, 
    allocate_franges:bool=False,
    **kwargs,
):

    try:
        
        # Загрузка словаря параметро в формате yaml
        processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))
        # Загрузка словаря наименований признаков и единиц измерения
        feature_names_dict = yaml.safe_load(open(path_to_feature_names_dict, encoding='utf-8'))
        # Загрузка словаря наименований диапазонов полета
        frange_names_dict = yaml.safe_load(open(path_to_frange_names_dict, encoding='utf-8'))

        # Подготовка директории для сохранения файла
        file_path_status = prepare_file_path(path_to_file=path_to_control_file)

        if path_to_source_file is not None:
            # Загрузка данных в формате parquet
            data_source = pd.read_parquet(
                path=path_to_source_file,
                engine='pyarrow'
            )
        else:
            data_source = None

        # Загрузка данных в формате parquet
        data_result = pd.read_parquet(
            path=path_to_result_file,
            engine='pyarrow'
        )

        # Загрузка признаков для построения контрольного изображения
        control_features = processing_params.get('control_features')
        # Разрешение контрольного изображения
        dpi = processing_params.get('dpi') if processing_params.get('dpi') is not None else 100
        # Выделение диапазонов полета на контрольном изображении
        allocate_franges = processing_params.get('allocate_franges') if processing_params.get('allocate_franges') is not None else False

        if allocate_franges is True:
            # Выделение диапазонов полета
            franges_data_dict = allocate_ranges(data=data_result, boundaries_only=True)
            takeoff_taxiings=franges_data_dict.get('takeoff_taxiings')
            takeoff_runs=franges_data_dict.get('takeoff_runs')
            forward_flights=franges_data_dict.get('forward_flights')
            landing_runs=franges_data_dict.get('landing_runs')
            landing_taxiings=franges_data_dict.get('landing_taxiings')
        else:
            takeoff_taxiings=[(None,None)]
            takeoff_runs=[(None,None)]
            forward_flights=[(None,None)]
            landing_runs=[(None,None)]
            landing_taxiings=[(None,None)]
        
        # Построение контрольных изображений
        control_file_list = control_plot(
            data_source=data_source,
            data_result=data_result,
            feature_list=control_features,
            feature_names=feature_names_dict,
            frange_names=frange_names_dict,
            takeoff_taxiings=takeoff_taxiings,
            takeoff_runs=takeoff_runs,
            forward_flights=forward_flights,
            landing_runs=landing_runs,
            landing_taxiings=landing_taxiings,
            dpi=dpi
        )

        # Сохранение контрольных изображений в файл
        save_figures_to_file(figure_list=control_file_list, path_to_save_file=path_to_control_file)

        # Статус обработки
        processing_status = True

    except Exception:

        # Статус обработки
        processing_status = False
    
    return processing_status