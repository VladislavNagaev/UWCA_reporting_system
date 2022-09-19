import os
from glob import glob

from typing import Literal, List, Union
import logging

def file_names_by_glob(path_to_glob):

    if isinstance(path_to_glob, list):
        path_to_file_list = path_to_glob

        if len(path_to_glob)>0:
            path_to_file = path_to_file_list[0]
            file = os.path.basename(path_to_file)
            file_format = ''.join(os.path.splitext(file)[-1:])[1:]
            file_base_path = os.path.dirname(path_to_file)

        else:
            file_base_path = None
            file_format = None

    else:
        path_to_file_list = glob(path_to_glob)
        file_format = ''.join(os.path.splitext(path_to_glob)[-1:])[1:]
        file_base_path = os.path.dirname(path_to_glob)

    file_list = [os.path.basename(path_to_file) for path_to_file in path_to_file_list]
    file_name_list = [''.join(os.path.splitext(file)[:-1]) for file in file_list]

    return file_name_list, file_base_path, file_format


def get_path_to_file_list(
    source_glob:Union[str,List[str]], 
    result_glob:Union[str,List[str]], 
    mode:Literal['difference', 'intersection']='difference',
) -> str:

    source_file_names, source_base_path, source_file_format = file_names_by_glob(path_to_glob=source_glob)
    result_file_names, result_base_path, result_file_format = file_names_by_glob(path_to_glob=result_glob)

    if mode == 'difference':
        raw_file_name_list = list(set(source_file_names).difference(set(result_file_names)))
    elif mode == 'intersection':
        raw_file_name_list = list(set(source_file_names).intersection(set(result_file_names)))

    raw_file_list = [file_name + '.' + source_file_format for file_name in raw_file_name_list]
    raw_path_to_file_list = [os.path.join(source_base_path, file) for file in raw_file_list]
    
    # Сортировка по времени добавления файла в директорию
    raw_path_to_file_list = sorted(raw_path_to_file_list, key=os.path.getmtime)
    
    return raw_path_to_file_list


