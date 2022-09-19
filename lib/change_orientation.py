from docx.enum.section import WD_ORIENT, WD_SECTION


# Функция изменения ориентации страницы документа (вертикальная/горизонтальная)
def change_orientation(document):

    current_section = document.sections[-1]
    new_width, new_height = current_section.page_height, current_section.page_width
    new_section = document.add_section(WD_SECTION.NEW_PAGE)
    new_section.orientation = WD_ORIENT.LANDSCAPE
    new_section.page_width = new_width
    new_section.page_height = new_height

    return document

