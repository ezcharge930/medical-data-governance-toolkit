
from structure.utils.file_utils import FileUtils
from structure.utils.db_utils import DBUtils
from structure.utils.config_utils import ConfigUtils
from structure.data.raw_data import DataReady
from structure.data.data_processor import DataProcessor
from structure.data.task_manager import TaskManager


# class MainApp:
#     @staticmethod
#     def run_project():
#         file_utils = FileUtils()
#         db_utils = DBUtils()
#         dataready = DataReady(file_utils, db_utils)
#         config_utils = ConfigUtils()
#         data_processor = DataProcessor(file_utils, db_utils, dataready)
#         data_processor.run_dm()
        

#     @staticmethod
#     def run_df_process():
#         config_utils = ConfigUtils()
#         taskmanager = TaskManager(config_utils)
#         taskmanager.run_ds_multi_process()
        

class Application:
    def __init__(self, project_name: str, config_utils: ConfigUtils) -> None:
        self.project_name = project_name
        self.config_utils = config_utils
        self.supported_datasets = self.config_utils.get_supported_dataset()
        self._initialize_components()
        
        
    def _initialize_components(self):
        '''# NOTE 初始化组件'''
        self.file_utils = FileUtils()
        self.db_utils = DBUtils()
        self.dataready = DataReady(self.file_utils, self.db_utils)
        self.dataprocessor = DataProcessor(self.file_utils, self.db_utils, self.dataready)
        self.task_manager = TaskManager(self.config_utils)
        
    def run_datasets(self, datasets: str|list[str]|None = None):
        ''' #NOTE 运行数据集处理'''
        dataset_list = self._get_dataset_list(datasets)
        print(f'准备执行数据集:{dataset_list}')

        for dataset_name in dataset_list:
            try:
                print(f'开始处理 {dataset_name.upper()} 数据集')
                self.dataprocessor.run_dataset(dataset_name)
                print(f'{dataset_name.upper()} 处理完成')
            except Exception as e:
                print(f'{dataset_name} 处理失败: {e}')
                continue

    def _get_dataset_list(self, datasets = None):
        ''' #NOTE 获取要执行的数据集列表'''
        if datasets is None or 'all' in datasets:
            config_datasets = self.config_utils.get_exe_ls()
            return config_datasets or self.config_utils.get_default_datasets()
        else:
            '''列表推导式
            [表达式 for 变量 in 可迭代对象 if 条件]
            等价于
            result = []
            for ds in datasets:
                if ds in self.supported_datasets:
                    result.append(ds)
            '''
            return [ds for ds in datasets if ds in self.supported_datasets] or self.config_utils.get_default_datasets()
    
    def run_parallel_tasks(self):
        """运行并行任务"""
        self.task_manager.run_ds_multi_process()
    