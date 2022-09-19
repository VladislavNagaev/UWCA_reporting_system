import numpy as np
import pandas as pd

from .get_frequency import get_frequency

from numpy import ndarray
from pandas import DataFrame
from typing import Tuple, Literal, Union, List

import logging


class allocate_flight_ranges():
    
    def __init__(self, data, params):
        self.data = data
        self.params = params
    

    def allocate_ranges(self, ) -> DataFrame:

        data = self.data
        params = self.params

        frange = params.get('frange')
        
        data_corr = data[[
            params.get('time'),
            params.get('horizontal_speed'),
            params.get('vertical_speed'),
            params.get('absolute_height'),
            params.get('overload_y'),
            params.get('overload_x'),
        ]].copy()
        
        data_corr = self.data_enrichment(data=data_corr, params=params)
        data_corr = self.allocate_ranges_ff(data=data_corr, params=params)
        data_corr = self.allocate_ranges_trff(data=data_corr, params=params)
        data_corr = self.allocate_ranges_fflr(data=data_corr, params=params)
        data_corr = self.allocate_boundary_trff(data=data_corr, params=params)
        data_corr = self.allocate_boundary_fflr(data=data_corr, params=params)
        data_corr = self.allocate_ranges_tr(data=data_corr, params=params)
        data_corr = self.allocate_ranges_lr(data=data_corr, params=params)
        data_corr = self.allocate_ranges_tttr(data=data_corr, params=params)
        data_corr = self.allocate_boundary_tttr(data=data_corr, params=params)
        data_corr = self.allocate_ranges_tt(data=data_corr, params=params)
        data_corr = self.allocate_ranges_lt(data=data_corr, params=params)
        
        data_corr = self.remove_boundaries(data=data_corr, params=params)
        data_corr = self.collect_frange(data=data_corr, params=params)

        data[frange] = data_corr[frange].values

        return data


    def data_enrichment(self, data:DataFrame, params:dict) -> DataFrame:
        
        frequency = get_frequency(data=data[params.get('time')].values)
        
        horizontal_speed = params.get('horizontal_speed')
        hspeed_increment_n5 = params.get('hspeed_increment_n5')
        
        vertical_speed = params.get('vertical_speed')
        vspeed_increment_n5 = params.get('vspeed_increment_n5')
        
        overload_x = params.get('overload_x')
        overload_x_increment_n15 = params.get('overload_x_increment_n15')
        overload_x_w100_increment_n10 = params.get('overload_x_w100_increment_n10')    
        
        overload_y = params.get('overload_y')
        overload_y_amplitude = params.get('overload_y_amplitude')
        overload_y_abs_w100_increment_n10 = params.get('overload_y_abs_w100_increment_n10')

        
        # Скорость роста горизонтальной скорости на следующих n точках (5 секундах)
        data[hspeed_increment_n5] = self.__get_param_increment(
            data=data[[horizontal_speed]], 
            n=5*frequency,
        )
        
        # Скорость роста вертикальной скорости на следующих n точках (5 секундах)
        data[vspeed_increment_n5] = self.__get_param_increment(
            data=data[[vertical_speed]], 
            n=5*frequency,
        )
        
        # Скорость роста перегрузки по оси X на следующих n точках (10 секундах)
        data[overload_x_w100_increment_n10] = self.__get_param_increment(
            data=data[[overload_x]].rolling(window=100).mean(), 
            n=10*frequency,
        )
        # Скорость роста перегрузки по оси X на следующих n точках (15 секундах)
        data[overload_x_increment_n15] = self.__get_param_increment(
            data=data[[overload_x]], 
            n=15*frequency,
        )    
        
        # Модуль амплитуды значений перегрузки по оси Y относительно нуля
        data[overload_y_amplitude] = data[overload_y].abs()
        # Скорость роста амплитуды перегрузки по оси Y на следующих n точках (10 секундах)
        data[overload_y_abs_w100_increment_n10] = self.__get_param_increment(
            data=data[[overload_y]].abs().rolling(window=100).mean(), 
            n=10*frequency,
        )
        
        return data


    def allocate_ranges_ff(self, data:DataFrame, params:dict) -> DataFrame:
        
        time = params.get('time')
        horizontal_speed = params.get('horizontal_speed')
        
        range_params = params.get('ff')
        
        range_name = range_params.get('range_name')
        hspeed = range_params.get('hspeed')
        duration = range_params.get('duration')
        
        data[range_name] = self.__get_ranges(
            hspeed_data=data[horizontal_speed].values,
            time_data=data[time].values,
            hspeed_range=hspeed,
            duration_range=duration,
        )
        
        return data


    def allocate_ranges_trff(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('trff')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_ranges_fflr(self, data:DataFrame, params:dict) -> DataFrame:

        range_params = params.get('fflr')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_boundary_trff(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('trff')
        range_ref_params = range_params.get('range_ref')
        sorting_list = range_params.get('sorting_list')
        
        data = self.__allocate_boundary(
            data=data,
            time=params.get('time'),
            range_name=range_params.get('range_name'),
            sorting_list=[(params.get(sf), ss, sw) for sf, ss, sw in sorting_list],
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
            sorting_sections=range_params.get('sorting_sections'),
            frequency=get_frequency(data=data[params.get('time')].values),
        )
        
        return data


    def allocate_boundary_fflr(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('fflr')
        range_ref_params = range_params.get('range_ref')
        sorting_list = range_params.get('sorting_list')
        
        data = self.__allocate_boundary(
            data=data,
            time=params.get('time'),
            range_name=range_params.get('range_name'),
            sorting_list=[(params.get(sf), ss, sw) for sf, ss, sw in sorting_list],
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
            sorting_sections=range_params.get('sorting_sections'),
            frequency=get_frequency(data=data[params.get('time')].values),
        )
        
        return data


    def allocate_ranges_tr(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('tr')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_ranges_lr(self, data:DataFrame, params:dict) -> DataFrame:

        range_params = params.get('lr')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_ranges_tttr(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('tttr')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_boundary_tttr(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('tttr')
        range_ref_params = range_params.get('range_ref')
        sorting_list = range_params.get('sorting_list')
        
        data = self.__allocate_boundary(
            data=data,
            time=params.get('time'),
            range_name=range_params.get('range_name'),
            sorting_list=[(params.get(sf), ss, sw) for sf, ss, sw in sorting_list],
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
            sorting_sections=range_params.get('sorting_sections'),
            frequency=get_frequency(data=data[params.get('time')].values),
        )
        
        return data


    def allocate_ranges_tt(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('tt')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def allocate_ranges_lt(self, data:DataFrame, params:dict) -> DataFrame:
        
        range_params = params.get('lt')
        range_ref_params = range_params.get('range_ref')
        
        data = self.__allocate_ranges(
            data=data,
            time=params.get('time'),
            horizontal_speed=params.get('horizontal_speed'),
            hspeed=range_params.get('hspeed'),
            duration=range_params.get('duration'),
            relative_range_width=range_ref_params.get('relative_range_width'),
            range_name=range_params.get('range_name'),
            range_name_ref=params.get(range_ref_params.get('range_name')).get('range_name'),
            boundary_side=range_ref_params.get('boundary_side'),
        )
        
        return data


    def remove_boundaries(self, data:DataFrame, params:dict) -> DataFrame:
        
        ranges = ['tt', 'tr', 'ff', 'lr', 'lt']
        range_names = [params.get(range_).get('range_name') for range_ in ranges]
        
        for range_name in range_names:
            range_names_list = list(set(range_names).difference({range_name, }))
            for range_number, range_data in data.groupby(by=range_name):
                if range_number != 0:
                    data.loc[range_data.index, range_names_list] = 0
        
        return data


    def collect_frange(self, data:DataFrame, params:dict) -> DataFrame:
        
        frange = params.get('frange')
        ranges = ['tt', 'tr', 'ff', 'lr', 'lt']

        data[frange] = ''

        for range_ in ranges:

            range_name = params.get(range_).get('range_name')
            frange_name = params.get(range_).get('frange_name')

            for range_number, range_data in data.groupby(by=range_name):
                if range_number != 0:
                    data.loc[range_data.index, frange] = frange_name + '-' + str(range_number)
                    
        return data


    def __get_param_increment(self, data, n:int=100):
        """
        Скорость роста параметра на последующих n точках
        """
        # Среднее значение на следующих n точках
        rolling = data.sort_index(ascending=False).rolling(window=n).mean()\
            .sort_index(ascending=True)
        # Приращение
        param_increment = (rolling-data).round(3)

        return param_increment
    

    def __allocate_ranges(
        self,
        data:DataFrame,
        time:str,
        horizontal_speed:str,
        hspeed:Tuple[float],
        duration:Tuple[float],
        relative_range_width:Tuple[float],
        range_name:str,
        range_name_ref:str,
        boundary_side:Literal['left','right'],
    ) -> DataFrame:
        
        # Зануление диапазонов
        data[range_name] = 0
        
        # Абсолютные значения ширины диапазона
        range_width = (relative_range_width[0]*duration[-1], relative_range_width[-1]*duration[-1])

        # Проход по референсным участкам
        for (range_number_ref, range_data_ref) in data.groupby(by=range_name_ref):
            if range_number_ref != 0:

                # Границы референсного
                left_boundary_ref = range_data_ref[time].iloc[0]
                right_boundary_ref = range_data_ref[time].iloc[-1]
                
                # Оцениваемая граница
                if boundary_side == 'left':
                    reference_boundary = left_boundary_ref
                elif boundary_side == 'right':
                    reference_boundary = right_boundary_ref
                
                # Границы диапазона
                left_boundary = reference_boundary - range_width[0]
                right_boundary = reference_boundary + range_width[-1]   
                
                # Участок данных, соотвествующий границам диапазона xx
                range_data = data.loc[
                    (data[time] >= left_boundary) &
                    (data[time] <= right_boundary) 
                ]

                # Уточненный по вертикальной скорости и продолжительности участок данных,
                # соотвествующий границам диапазона xx
                range_data.loc[:, range_name] = self.__get_ranges(
                    hspeed_data=range_data[horizontal_speed].values,
                    time_data=range_data[time].values,
                    hspeed_range=hspeed,
                    duration_range=duration,
                )

                # Удаление коллизий, возникших за счет разрыва участка xx
                # (выбор участка, содержащего референсную границу диапазона горизонтального полета)
                for (range_number_i, range_data_i) in range_data.groupby(by=range_name):
                    if range_number_i != 0:

                        # Границы диапазона xx
                        left_boundary_i = range_data_i[time].iloc[0]
                        right_boundary_i = range_data_i[time].iloc[-1]

                        if (
                            (reference_boundary < left_boundary_i) or 
                            (reference_boundary > right_boundary_i)
                        ):

                            # Зануление диапазона, не содержащего референсной границы
                            range_data.loc[
                                range_data[range_name] == range_number_i, [range_name]
                            ] = 0    

                        else:

                            # Переименование индекса диапазона xx на индекс диапазона соотвествуюшего рефернсного
                            range_data.loc[
                                range_data[range_name] == range_number_i, [range_name]
                            ] = range_number_ref

                # Возврат локальных данных из массива range_data в глобальный массив
                data.loc[
                    (data[time] >= left_boundary) & (data[time] <= right_boundary), 
                    [range_name]
                ] = range_data.loc[:,[range_name]]    
        
        return data


    def __get_ranges(
        self,
        hspeed_data:ndarray,
        time_data:ndarray,
        hspeed_range:Tuple[float],
        duration_range:Tuple[float],
    ) -> ndarray:

        local_data = pd.DataFrame()
        local_data['hspeed'] = hspeed_data
        local_data['time'] = time_data
        local_data['range_name'] = 0

        # Попадение данных в диапазон по скорости
        local_data['range'] = (
            (local_data['hspeed'] >= hspeed_range[0]) & 
            (local_data['hspeed'] <= hspeed_range[-1])
        )

        # Удаление выбросов (единичное значение, соотвествующее условиям)
        local_data.loc[
            (
                local_data['range'] & (~(
                    local_data['range'].shift(periods=1).fillna(False) | 
                    local_data['range'].shift(periods=-1).fillna(False)
                ))
            )   
            , ['range']
        ] = False    

        local_data['range_boundary'] = (
            (local_data['range'] != local_data['range'].shift(periods=1)) | 
            (local_data['range'] != local_data['range'].shift(periods=-1))
        )

        range_boundarys = local_data[local_data['range_boundary']]['range_boundary'].index

        for i, (left_boundary, right_boundary) in enumerate(range_boundarys.values.reshape(-1,2)):

            # Длительность диапазона
            duration = local_data.loc[right_boundary,'time'] - local_data.loc[left_boundary,'time']

            if (
                # 1. Все элементы участка попадают в диапазон
                ((~local_data.loc[left_boundary:right_boundary, 'range']).sum() == 0) and
                # 2. Длительность диапазона попадает в установленные границы
                ((duration >= duration_range[0]) and (duration <= duration_range[-1]))
            ):
                # Нумерация диапазона
                local_data.loc[left_boundary:right_boundary, ['range_name']] = i+1

        return local_data['range_name'].values


    def __allocate_boundary(
        self,
        data:DataFrame, 
        time:str,
        range_name:str,
        sorting_list:List[Tuple[Union[str,float]]],
        range_name_ref:str,
        boundary_side:Literal['left','right'],
        sorting_sections:List[float],
        frequency:int=50,
    ) -> DataFrame:
        
        # Корректировка границ референсного участка на оцениваемом участке
        for (range_number, range_data) in data.groupby(by=range_name):
            if range_number != 0:
                
                range_data_i = range_data

                # Длительность секции в секундах
                for n in sorting_sections:
                    
                    # Число элементов в секции
                    section = np.int(n*frequency)

                    # Список признаков, по которым ведется сортировка
                    sort_features = [sort[0] for sort in sorting_list]
                    
                    # Массив средних положений секций
                    section_data = range_data_i.iloc[np.int(section / 2)::section]

                    for sort_feature, sort_state, sort_weight in sorting_list:
                        
                        # Массив данных по участкам
                        section_array = self.__split_into_sections(
                            data=range_data_i[sort_feature].values,
                            section=section,
                            func=sort_state,
                        )

                        # Добавление значения в глобальный массив
                        section_data.loc[:, sort_feature] = section_array[np.int(section / 2)::section]
                        feature_data = section_data[[sort_feature]]
                        feature_data_normalize = (feature_data - feature_data.min()) / (feature_data.max() - feature_data.min())

                        if sort_state == 'min':
                            feature_data_normalize = 1 - feature_data_normalize 
                        
                        section_data.loc[:, sort_feature] = feature_data_normalize * sort_weight

                    # Определение границы перехода между участками
                    boundary_index = section_data[sort_features].sum(axis=1).sort_values(ascending=False).index[0]
                    boundary_time = range_data_i.loc[boundary_index, time]

                    # Переопределение участка поиска
                    range_data_i = range_data_i.loc[boundary_index-section/2:boundary_index+section/2]

                if boundary_side == 'left':
                    nullable_side = data[time] < boundary_time
                elif boundary_side == 'right':
                    nullable_side = data[time] > boundary_time

                # Корректировка референсного диапазона (зануление данных не вошедших в диапазон)
                data.loc[
                    (data[range_name_ref]==range_number) & nullable_side,
                    [range_name_ref]
                ] = 0     
        
        return data


    def __split_into_sections(self, data:ndarray, section:int, func:str='mean') -> ndarray:
        
        data_shape = data.shape[0]
        
        integer_part = data_shape // section
        remainder_part = data_shape % section
        
        integer_part_shape = integer_part*section
        remainder_part_shape = remainder_part
        
        integer_part_data = data[:integer_part_shape]
        remainder_part_data = data[-remainder_part_shape:]
        
        indices_or_sections = np.int(integer_part_shape / section)
        
        integer_part_data = np.split(
            integer_part_data,
            indices_or_sections=indices_or_sections,
            axis=0
        )
        integer_part_data = np.array(integer_part_data)
        
        
        if func == 'mean':
            integer_part_data = np.mean(integer_part_data, axis=1)
            remainder_part_data = np.mean(remainder_part_data, axis=0)
            
        elif func == 'median':
            integer_part_data = np.median(integer_part_data, axis=1)
            remainder_part_data = np.median(remainder_part_data, axis=0)
            
        elif func == 'max':
            integer_part_data = np.max(integer_part_data, axis=1)
            remainder_part_data = np.max(remainder_part_data, axis=0)
        
        elif func == 'min':
            integer_part_data = np.min(integer_part_data, axis=1)
            remainder_part_data = np.min(remainder_part_data, axis=0)
            
        else:
            raise ImportError()
        
        integer_part_data = np.repeat(integer_part_data, repeats=section, axis=0)
        remainder_part_data = np.repeat(remainder_part_data, repeats=remainder_part_shape, axis=0)
        
        sections_data = np.append(integer_part_data, values=remainder_part_data)
        
        return sections_data




