

from src.utils.config_service import ConfigurationService

from src.data.raw_data import DataLoader
from src.data.data_processor import DataProcessor

from src.domain.dm import DM
        

class Application:
    def __init__(self, project_name: str) -> None:
        self.project_name = project_name
        self.config_service =  ConfigurationService()
        self.supported_datasets = self.config_service.load_json_config('datasets.json')
        self._initialize_components()
        
        
        
    def _initialize_components(self):
        '''# NOTE 初始化组件'''

        self.dataloader = DataLoader()
        self.dataprocessor = DataProcessor(self.dataloader)
        # 注册DM集
        self.dataprocessor.register_handler(DM())
        
    def run_datasets(self, datasets: str|list[str]|None = None):
        ''' #NOTE 运行数据集处理'''
        dataset_list: list[str] = self._get_dataset_list(datasets)
        print(f'准备执行数据集:{dataset_list}')

        for dataset_name in dataset_list:
            try:
                print(f'开始处理 {dataset_name.upper()} 数据集')
                self.dataprocessor.run_dataset(dataset_name, self.config_service)
                print(f'{dataset_name.upper()} 处理完成')
            except Exception as e:
                print(f'{dataset_name} 处理失败: {e}')
                continue

    def _get_dataset_list(self, datasets: str|list[str]|None = None) -> list[str]:
        ''' #NOTE 获取要执行的数据集列表'''
        ''' #NOTE 如果dataset为空或者all,当配置文件中写明了输出数据集,则输出
                  如果配置文件中没有写,但是dataset=all,输出默认数据集
                  如果配置文件中没有写,同时dateset=None,输出默认数据集
        '''
 
        if datasets is None or (isinstance(datasets, list) and 'all' in datasets):
            config_datasets = self.config_service.get_execution_datasets()
            if config_datasets:
                return config_datasets
            return ['dm']
        elif isinstance(datasets, str):
            datasets = [datasets]
        # 过滤支持的数据集(可从config_service获取支持列表)
        # supported_dataset = self.config_service.load_json_config('datasets.json')
        return [d for d in datasets if d in self.supported_datasets] or ['dm']
            
        