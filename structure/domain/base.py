from abc import ABC, abstractmethod
from structure.data.raw_data import DataReady

class DatasetHandler(ABC):
    @abstractmethod
    def process(self, dataready: DataReady):
        ''' 处理数据集'''
        pass
    
    @abstractmethod
    def get_dataset_name(self) -> str:
        ''' 返回支持的数据集名,如'dm', 'sv'等'''
        pass