import numpy as np
from numpy import ndarray


def get_frequency(data:ndarray) -> int:
    """
    Вычисляет частоту дискретизации данных
    """
    
    # frequency = np.int(np.round(np.mean(1 / np.diff(data)), decimals=0))
    frequency = np.int(np.round(1 / (data[1] - data[0]), decimals=0))
    
    return frequency
