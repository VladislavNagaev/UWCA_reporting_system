import docx
from docx.enum.text import WD_ALIGN_PARAGRAPH
from matplotlib import pyplot
import io
import gc


# Функция сохранения фигуры в документ
def save_figure_to_docx(document, figure):
    
    # Сохранение графика в буфер
    buffer = io.BytesIO()
    figure.savefig(buffer, format='png', bbox_inches='tight')
    buffer.seek(0)       
    
    # Добавление графика в документ word
    d = document.add_picture(buffer, width = docx.shared.Cm(16))
    d.alignment=WD_ALIGN_PARAGRAPH.CENTER
    
    # Очистка буфера
    buffer.close()
    
    # Закрытие графика
    pyplot.close()     
    
    # Очистка мусора
    gc.collect()
    
    return document