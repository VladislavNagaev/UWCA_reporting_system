import io
import gc
from PIL import Image
from matplotlib import pyplot
from typing import List


def save_figures_to_file(figure_list:List, path_to_save_file:str):

    images = list()

    for figure in figure_list:

        # Сохранение графика в буфер
        buffer = io.BytesIO()
        figure.savefig(buffer, format='png', dpi='figure', bbox_inches='tight')
        buffer.seek(0)       

        # Загрузка изображения
        image = Image.open(buffer)
        image.load()

        # Преобразование RGBA в RGB
        image_c = Image.new("RGB", image.size, (255, 255, 255))
        image_c.paste(image, mask=image.split()[3]) # 3 is the alpha channel

        # Сохранение графика в список
        images.append(image_c)

        # Очистка буфера
        buffer.close()

        # Закрытие графика
        pyplot.close()     

    # Очистка мусора
    gc.collect()

    images[0].save(fp=path_to_save_file, save_all=True, append_images=images[1:])

    return True

