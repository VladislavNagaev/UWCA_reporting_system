import docx

from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.enum.table import WD_TABLE_ALIGNMENT, WD_CELL_VERTICAL_ALIGNMENT, WD_ROW_HEIGHT_RULE
from docx.oxml import OxmlElement
from docx.oxml.ns import qn
from docx.table import _Cell
from docx.shared import Mm

import numpy as np



def set_vertical_cell_direction(cell: _Cell, direction: str):
    # direction: tbRl -- top to bottom, btLr -- bottom to top
    assert direction in ("tbRl", "btLr")
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    textDirection = OxmlElement('w:textDirection')
    textDirection.set(qn('w:val'), direction)  # btLr tbRl
    tcPr.append(textDirection)




# Функция сохранения таблицы в документ
def save_pd_table_to_docx(document, table, index=None, columns=None, top_left_corner=None, vertical_columns=False,):
    """
    Принимает документ docx, таблицу numpy array, вектор/список наименований строк, 
    вектор/список наименований столбцов, наименование верхнего левого угла.
    
    Возвращает документ с добавленной таблицей.
    """
    
    # Предупреждение о несоответствии заданных переменных
    if index is not None and top_left_corner is not None and columns is None:
        print('Переменная top_left_corner не будет отображена, так как не задана переменная columns.')
    if columns is not None and top_left_corner is not None and index is None:
        print('Переменная top_left_corner не будет отображена, так как не задана переменная index.')
    if top_left_corner is not None and index is None and columns is None:
        print('Переменная top_left_corner не будет отображена, так как не заданы переменные index и columns.')

    # Преобразование table, index, columns, top_left_corner к формату numpy array
    if table is not None:
        table = np.array(table)
    if index is not None:
        index = np.array(index)
    if columns is not None:
        columns = np.array(columns)
    if top_left_corner is not None:
        top_left_corner = np.array(top_left_corner)
        
    # Проверка размерностей переменных и reshape в вектор
    if index is not None:
        if len(index.shape) == 1:
            if index.shape[0] != table.shape[0]:
                raise ValueError('Количество строк таблицы и количество переменных в index должно совпадать.')
        elif len(index.shape) == 2:
            if index.shape[0] == 1 or index.shape[1] == 1:
                index = index.reshape(1,-1)[0,:]
                if index.shape[0] != table.shape[0]:
                    raise ValueError('Количество строк таблицы и количество переменных в index должно совпадать.')
            else:
                raise ValueError('Переменная index должна быть вектором.')
        else:
            raise ValueError('Переменная index должна быть вектором.')
    if columns is not None:
        if len(columns.shape) == 1:
            if columns.shape[0] != table.shape[1]:
                raise ValueError('Количество столбцов таблицы и количество переменных в columns должно совпадать.')
        elif len(columns.shape) == 2:
            if columns.shape[0] == 1 or columns.shape[1] == 1:
                columns = columns.reshape(1,-1)[0,:]
                if columns.shape[0] != table.shape[1]:
                    raise ValueError('Количество столбцов таблицы и количество переменных в columns должно совпадать.')
            else:
                raise ValueError('Переменная columns должна быть вектором.')
        else:
            raise ValueError('Переменная columns должна быть вектором.')

    # Подготовка данных для записи
    if index is not None and columns is not None:
        # Задание значения переменной top_left_corner по-умолчанию
        if top_left_corner is None:
            top_left_corner = np.array([''])
        # Объединение объединенныйх таблиц (top_left_corner и columns) и (index и table)
        data_for_record = np.vstack((
            # Объединение таблиц top_left_corner и columns
            np.hstack((top_left_corner.reshape(1,-1), columns.reshape(1,-1))),
            # Объединение таблиц index и table
            np.hstack((index.reshape(-1,1), table))
        ))
    elif index is not None and columns is None:
        # Объединение таблиц index и table
        data_for_record = np.hstack((index.reshape(-1,1), table))
    elif index is None and columns is not None:
        # Объединение таблиц top_left_corner и columns
        data_for_record = np.vstack((columns.reshape(1,-1), table))
    else:
        data_for_record = table

    # Создание объекта стиля
    style = document.styles['Table Grid']
    # Определение параметров объекта стиля
    font = style.font
    font.name = 'Times New Roman'
    font.size = docx.shared.Pt(12)      
    
    # Создание таблицы в документе
    doc_table = document.add_table(rows = data_for_record.shape[0], cols = data_for_record.shape[1])
    # Стиль для таблицы в документе
    doc_table.style = 'Table Grid'
    # Выравнивание
    doc_table.alignment = WD_TABLE_ALIGNMENT.CENTER
    # Автоподбор ширины
    doc_table.autofit = True
    # Выравнивание высоты ячеек
    doc_table.rows[0].height_rule = WD_ROW_HEIGHT_RULE.AUTO

    # Заполнение данных таблицы в документе
    for row in range(data_for_record.shape[0]):
        for col in range(data_for_record.shape[1]):
            # Получение ячейки таблицы в документе
            cell = doc_table.cell(row, col)
            # Получение данных из таблицыы для записи в ячейку в документе
            value = data_for_record[row, col]
            # Запись данных в ячейку
            cell.text = str(value)
            cell.vertical_alignment = WD_CELL_VERTICAL_ALIGNMENT.CENTER
            # Создание объекта стиля параграфа
            paragraph = cell.paragraphs[0]
            # Определение параметров объекта стиля параграфа
            paragraph.alignment = WD_ALIGN_PARAGRAPH.CENTER
            # Создание объекта стиля текста
            font = paragraph.runs[0].font
            # Определение параметров объекта стиля текста
            font.name = 'Times New Roman'
            font.size = docx.shared.Pt(12)
    
    if vertical_columns is True:
        row = 0
        for col in range(1, data_for_record.shape[1]):
            # Получение ячейки таблицы в документе
            cell = doc_table.cell(row, col)
            set_vertical_cell_direction(cell=cell, direction='btLr')
        # Выравнивание высоты ячеек
        doc_table.rows[row].height_rule = WD_ROW_HEIGHT_RULE.AUTO
        
    return document