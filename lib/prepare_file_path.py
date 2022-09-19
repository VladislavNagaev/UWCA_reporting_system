import os


def prepare_file_path(path_to_file:str):
    """
    Подготовка директории файла
    """
    
    # Директория файла
    PATH_TO_SAVE = os.path.dirname(path_to_file)

    # Проверка существования дирректории, в случае отсутствия - рекурсивно создает ее
    if not os.path.exists(PATH_TO_SAVE):
        try:
            os.makedirs(PATH_TO_SAVE)
        except Exception:
            return False
    
    return True
