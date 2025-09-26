/src/cli/parser.py

import argparse

class ArgumentParser:
    @staticmethod
    def parse_command_line() -> argparse.Namespace:
        '''
        使用argparse解析命令行参数
        返回解析后的参数命名空间
        '''
        # NOTE 创建解析器对象
        parser = argparse.ArgumentParser(
            description= '医保数据处理工具',
            epilog= '示例方法: python -m structure.main -p project -s -d dm mh'
        )
        
        # NOTE 添加项目参数(必需参数)
        parser.add_argument('--project', '-p', required= True, help= '指定要执行的项目名称')

        # NOTE 添加各种功能标志参数
        parser.add_argument('--save_resolved_query', '-s', action= 'store_true', help= '保存已解析的查询')
        parser.add_argument('--generate_query_work', '-g', action= 'store_true', help= '生成查询工作')
        parser.add_argument('--export_query_report', '-e', action= 'store_true', help= '导出查询报告')
        parser.add_argument('--datasets', '-d', nargs= '*', help= '指定要执行的数据集类型,如:dm dg'
                                                                    '使用 all 执行配置文件中的所有数据集')

        # NOTE 添加其他短选项标志(可选功能,未来可能会用到)
        parser.add_argument('-c', action= 'store_true', help= '选项c')
        parser.add_argument('-a', action= 'store_true', help= '选项a')
        parser.add_argument('-q', action= 'store_true', help= '选项q')
        parser.add_argument('-t', action= 'store_true', help= '选项t')
        parser.add_argument('-b', action= 'store_true', help= '选项b')
        parser.add_argument('-m', action= 'store_true', help= '选项m')
        parser.add_argument('-r', action= 'store_true', help= '选项r')

        return parser.parse_args()

/src/cli/runner.py

from src.cli.parser import ArgumentParser
from src.core.app import Application

class ApplicationRunner:
    ''' #NOTE 应用执行器'''
    def run(self):
        ''' #NOTE 运行应用'''
        try:
            # 解析参数
            args = ArgumentParser.parse_command_line()
            print(f'启动项目:{args.project}')
            
            # 创建应用实例
            app = Application(args.project)
            
            # 根据参数执行不同的功能
            self._execute_main_task(app= app, args= args)
            
            print('所有任务执行完成')
            
        except KeyboardInterrupt:
            print('用户中断程序')
        except Exception as e:
            print('程序出错:{e}')
            import traceback
            traceback.print_exc()
            
    def _execute_main_task(self, app: Application, args):
        ''' 执行主要任务'''
        if args.save_resolved_query or args.generate_query_work or args.export_query_report:
            app.run_datasets(datasets= args.datasets)
        else:
            print('请指定操作标志,如: -s -g -e')
            raise SystemExit(1)
        
/src/core/app.py


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
            
        
/src/output_arrange.py
import pandas as pd
import datetime

class OutputArranger:
    ''' 标准化输出整理器 '''
    def __init__(
        self,
        spec_columns: list[str],
        subjid_col: str = 'subjid',
        clean_chars: list[str]|None = None,
        add_timestamp: bool = True,
        timestamp_col: str = 'create_time',
        tiemstamp_value: datetime.date|None = None
    ) -> None:
        self.spec_columns = spec_columns
        self.subjid_col = subjid_col
        self.clean_chars = clean_chars or ['\n', '@']
        self.add_timestamp = add_timestamp
        self.timestamp_col = timestamp_col
        self.timestamp_value = tiemstamp_value or datetime.date.today()
        
    def arrange(self, raw_df: pd.DataFrame) -> pd.DataFrame:
        ''' 按规范整理输出 '''
        # 1、按规范重排字段
        df = raw_df.reindex(columns= self.spec_columns, fill_value= '')

        # 2、清洗字符串
        clean_df = df.map(self._clean_value)
        
        # 3、过滤无效记录
        filter_df = self._filter_valid_records(clean_df)
        
        # 4、去重
        df = filter_df.drop_duplicates()
        
        # 5、添加时间戳
        if self.add_timestamp:
            df[self.timestamp_col] = self.timestamp_value
            
        return df
        
        
        
    def _clean_value(self, val):
        if isinstance(val, str):
            cleaned = val.strip()
            for char in self.clean_chars:
                cleaned = cleaned.replace(char, '')
            return cleaned
        return val
    
    def _filter_valid_records(self, df: pd.DataFrame) -> pd.DataFrame:
        mask = (df[self.subjid_col] != '') & (pd.notnull(df[self.subjid_col]))
        result = df[mask].copy()
        return result


 /src/data/data_processor.py
from src.utils.file_utils import FileUtils
from src.utils.config_service import ConfigurationService


from src.data.raw_data import DataLoader
from src.domain.dm import DatasetHandler


import pandas as pd

class DataProcessor:
    def __init__(self,  dataloader:DataLoader) -> None:
        self.dataloader = dataloader
        self._handler: dict[str, DatasetHandler] = {}
        
    def register_handler(self, handler: DatasetHandler):
        ''' #NOTE 注册一个数据集处理器'''
        name = handler.get_dataset_name().lower()
        self._handler[name] = handler
        
    def run_dataset(self, dataset_name: str, config_service: ConfigurationService):
        # ''' #NOTE 运行指定的数据集处理'''
        dataset_name = dataset_name.lower()
        handler = self._handler.get(dataset_name)
        
        if not handler:
            raise ValueError(f'不支持的数据集: {dataset_name}. 已注册: {list(self._handler.keys())}')
        
        try:
            print(f'开始处理 {dataset_name.upper()} 数据集')
            result_df: pd.DataFrame = handler.process(self.dataloader, config_service)
            self.save_result(result_df, dataset_name)
            print(f'{dataset_name.upper()} 处理完成')
        except Exception as e:
            print(f'{dataset_name.upper()} 处理失败: {e}')
            raise

    def save_result(self, df:pd.DataFrame, dataset_name:str):
        file_utils = FileUtils()
        file_utils.save_df_by_timestamp(df, prefix= dataset_name)


/src/data/raw_data.py
import pandas as pd 


from typing import Protocol


class DataReader(Protocol):
    def read_raw_table(self, table_name: str, columns: list[str]) -> pd.DataFrame:
        ...

class DataLoader:
    def __init__(self) -> None:
        ...
        
    def read_raw_table(self, table_name: str, columns: list[str]) -> pd.DataFrame:
        ...

/src/data/task_manager.py
from src.utils.config_service import ConfigurationService

class TaskManager():
    def __init__(self, config_service: ConfigurationService) -> None:
        self.config_service = config_service
    
    def run_ds_multi_process(self):
        ...
    
    def process_main_task_ds(self):
        pass
    
    def combine_bath_df(self):
        pass
    
/src/domain/base.py
from abc import ABC, abstractmethod
from src.data.raw_data import DataLoader,DataReader
from src.utils.config_service import ConfigurationService
import pandas as pd

class DatasetHandler(ABC):
    @abstractmethod
    def process(self, dataloader: DataLoader, config_service: ConfigurationService) -> pd.DataFrame:
        ''' 处理数据集,接受配置服务以便获取全局或者数据集专属配置 '''
        ...
    
    @abstractmethod
    def get_dataset_name(self) -> str:
        ''' 返回支持的数据集名,如'dm', 'sv'等'''
        ...
    
    def _process_core_table(self, raw_data: dict[str, pd.DataFrame], config_df: pd.DataFrame) -> pd.DataFrame:
        ...
    
    
    def _load_raw_data(self, datareader: DataReader, config_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        raw_data: dict[str, pd.DataFrame] = {}
        # 获取所有唯一的"程序中数据集ID"
        for table_id in config_df['程序中数据集ID'].unique():
            # 找到对应 table_id 的配置
            table_config = config_df[config_df['程序中数据集ID'] == table_id]
            source_table = table_config['来源表']
            source_columns = table_config['来源表变量'].tolist()
            raw_data[table_id] = datareader.read_raw_table(source_table, source_columns)
        return raw_data  

            
    def _apply_where_condition(self, df: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
        ''' 应用 where 条件'''
        for _, row in config.iterrows():
            if row['where语句']:
                # 替换 {$} 为实际列名
                condition = row['where语句'].replace('{$}', row['来源表变量'])
                try:
                    df = df.query(condition)
                except Exception as e:
                    print(f'where 条件错误:{condition}, 错误: {e}')
        return df
                    
                    
    def _rename_columns(self, df: pd.DataFrame, config: pd.DataFrame) -> pd.DataFrame:
        ''' 重命名列 '''
        rename_map: dict[str, str] = {}
        for _, row in config.iterrows():
            if row['重命名变量']:
                rename_map[row['来源表变量']] = row['重命名变量']
        return df.rename(columns= rename_map)
    

/src/domain/dm.py

from typing import TYPE_CHECKING
import pandas as pd

from src.data.raw_data import DataReader
from src.domain.base import DatasetHandler
from src.utils.config_service import ConfigurationService
from src.core.output_arrange import OutputArranger


class DM(DatasetHandler):
    
    def get_dataset_name(self) -> str:
        return 'dm'
    
    def process(self, datareader: DataReader, config_service: ConfigurationService):
       
        whole_config = config_service.get_whole_config()
        print(f'全局配置: {whole_config}')

        config_df = config_service.get_dataset_config('dm')

        # 2、读取原始数据
        raw_data = self._load_raw_data(datareader, config_df)

        # 3、核心加工
        dm_table = self._process_core_table(raw_data, config_df)

        # 4、读取数据说明书
        spec_df: pd.DataFrame = config_service.get_specification(domain= 'DM')
        
        # 创建整理器
        arranger = OutputArranger(
            spec_columns= spec_df['变量名'].tolist(),
            subjid_col= 'subjid',
            clean_chars= ['\n', '@', '\r'],
            add_timestamp= True
        )
        
        final_dm = arranger.arrange(dm_table)

        return final_dm
    
    def _process_core_table(self, raw_data: dict[str, pd.DataFrame], config_df: pd.DataFrame) -> pd.DataFrame:
        # final_dm_table
        all_dfs: list = []

        for table_id in config_df['程序中数据集ID'].unique():
            if table_id not in raw_data:
                continue
            
            df = raw_data[table_id].copy()
            
            # 获取 table_id 的配置
            table_config = config_df[config_df['程序中数据集ID'] == table_id]

            # 1、应用where条件
            df = self._apply_where_condition(df, table_config)
            
            # 2、重命名列
            df = self._rename_columns(df, table_config)
            
            all_dfs.append(df)
            
        merge_df = pd.concat(all_dfs, ignore_index= True)

        # 患者数据聚合
        patient_df = self._map_patient_data(merge_df)
        
        return patient_df
      

    def _map_patient_data(self, df: pd.DataFrame) -> pd.DataFrame:
        ''' 按患者聚合数据 '''
        # 确保列存在
        required_cols: list = ['subjid', 'sex', 'brthdtc', 'height', 'weight']
        for col in required_cols:
            if col not in df.columns:
                print(f'警告：缺少列: {col}')
                return pd.DataFrame()
        # 聚合
        result = df.groupby('subjid').agg({
            'sex': self._map_sex,
            'brthdtc': self._map_brthdtc,
            'height': self._map_height_weight,
            'weight': self._map_height_weight
        }).reset_index()
        
        result['bmi'] = self._calculate_bmi(result['weight'], result['height'])
        return result
        
    def _map_sex(self, series: pd.Series) -> str:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return ''
    
    def _map_brthdtc(self, series: pd.Series) -> str:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return min(series)
        
    def _map_height_weight(self, series: pd.Series) -> float:
        mode_values = series.mode()
        if len(mode_values) == 1:
            return mode_values.iloc[0]
        else:
            return series.mean()
        
        
    def _calculate_bmi(self, weight: pd.Series, height: pd.Series) -> pd.Series:
        ''' 计算BMI '''
        weight = pd.to_numeric(weight, errors= 'coerce')  
        height = pd.to_numeric(height, errors= 'coerce')/ 100
        bmi = weight / (height ** 2)
        return bmi
        
/src/utils/config_service.py
import os
import json
import pandas as pd

class ConfigurationService:
    '''统一的配置服务,解耦配置加载与使用'''

    def __init__(self, config_dir: str|None = None) -> None:
        if config_dir is None:
            # 默认 config 目录: project_root/config
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            self.config_dir = os.path.join(project_root, 'config')
        else:
            self.config_dir = config_dir
            
    def load_json_config(self, filename: str) -> dict[str, dict]:
        ''' 加载 JSON 配置文件'''
        path = os.path.join(self.config_dir, filename)
        with open(path, 'r', encoding= 'utf-8') as f:
            return json.load(f)
        
    def load_excel_config(self, filename: str, sheet_name: str|int = 0) -> pd.DataFrame:
        ''' 加载 EXCEL 配置文件'''
        spec_path = os.path.join(self.config_dir, filename)
        spec_file = pd.read_excel(spec_path, sheet_name= sheet_name, dtype= object, keep_default_na= False).fillna('')
        return spec_file
    
    def get_whole_config(self) -> dict[str, dict]:
        ''' 获取 whole_config.xlsx 的第一行作为dict'''
        df = self.load_excel_config('whole_config.xlsx')
        return df.iloc[0].to_dict()

    def get_execution_datasets(self) -> list[str]:
        ''' 获取要执行的数据集列表 '''
        df = self.load_excel_config('whole_config.xlsx', sheet_name= '数据集执行控制')
        if '数据集代号' not in df.columns or '是否执行' not in df.columns:
            raise ValueError('配置表缺少必要列')
        return df[df['是否执行'] == 'y']['数据集代号'].tolist()
    
    def get_dataset_config(self, dataset_name: str) -> pd.DataFrame:
        ''' 获取特定数据集的配置(如:dm_raw_data_config.xlsx) '''
        filename = f'{dataset_name}_rawdata_config.xlsx'
        return self.load_excel_config(filename)
    
    def get_specification(self, domain: str) -> pd.DataFrame:
        ''' 读取数据说明书,并返回输出字段与顺序'''
        spec_path: pd.DataFrame = self.load_excel_config('whole_config.xlsx', sheet_name= 'whole_config')

        spec_df = pd.read_excel(spec_path, sheet_name= '计算规则', dtype= object, keep_default_na= False).fillna('')
        
        spec_df = spec_df.loc[(spec_df['数据集'] == domain) & (spec_df['输出标志'].str.lower() == 'y')]

        return spec_df


/src/utils/db_utils.py
class DBUtils:
    def create_model_class(self):
        pass
    
    def execute_raw_data_import_sqlite(self):
        pass
    
    def read_spec_df(self):
        pass
    
    def arrange_out_df(self):
        pass
    

/src/utils/file_utils.py
import os
import pandas as pd
import datetime

class FileUtils:
    def __init__(self, project_name: str|None = None) -> None:
        self.project_name = project_name
        # NOTE structure/utils
        self.current_dir = os.path.dirname(os.path.abspath(__file__))
        # NOTE structure
        self.structure_dir = os.path.dirname(self.current_dir)
    
    # NOTE 目前的项目暂时不需要去运行多个项目,就先基于本项目目录开始
    def get_raw_data_path(self, config_file: str | None, path: str) -> str:
        '''
        获取原始数据文件路径
        
        Args:
            config_file: 配置文件名
            path: 相对于stucture目录的路径
            
        Returns:
            完整的文件路径
        '''
        print(f"DEBUG: config_file={config_file}, path={path}")
        print(f"DEBUG: current_dir={self.current_dir}")
        print(f"DEBUG: structure_dir={self.structure_dir}")
        # 特殊处理config路径 - 指向stucture/config
        if path and 'config' in path.lower():
            # NOTE 构建 stucture/config 路径
            config_dir = os.path.join(self.structure_dir, 'config')
        else:
            # 其他情况，基于stucture目录
            if path:
                if not os.path.isabs(path):
                    config_dir = os.path.join(self.structure_dir, path)
                else:
                    config_dir = path
            else:
                config_dir = self.structure_dir
        
        print(f"DEBUG: config_dir={config_dir}")
        
        # 确保目录存在
        os.makedirs(config_dir, exist_ok=True)
        
        if config_file:
            # NOTE structure/config/xxxxx
            datafile = os.path.join(config_dir, config_file)
        else:
            datafile = config_dir
            
        print(f"DEBUG: 最终返回路径={datafile}")
        return datafile
    
    def get_all_raw_data_path(self):
        pass
    
    def read_csv_to_pd(self):
        pass
    
    def save_df_by_timestamp(self, df: pd.DataFrame, prefix: str = 'output'):
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'{prefix}_{timestamp}.csv'
        filepath = os.path.join(self.structure_dir, 'output', filename)
        os.makedirs(os.path.dirname(filepath), exist_ok= True)
        df.to_csv(filepath, index= False,  encoding= 'utf-8')
        print(f'已保存: {filepath}')
 
/src/main.py

from src.cli.runner import ApplicationRunner

def main():
  
    runner = ApplicationRunner()
    runner.run()


if __name__ == '__main__':
    
    main()
    






1.当配置项增多时，可以使用更轻量化的配置文件，例如json或者yaml
一方面json或者yaml相对于excel来说更加的便捷，在python中操作也更方便
另外json和yaml可以支持更复杂的配置方式，例如给某个变量实现某种函数处理等等 
2.存在代码注入的风险，目前对于传入的{$}没有进行字符判断，如果用户写入恶意表达式，会影响整体


1.如果一个程序需要跑几个小时甚至几天，然后处理到这一步的时候返回空dataframe，无疑是资源的极大浪费
2.更合理的错误处理方式应该是在最开始读取配置文件的时候直接进行判断，如果发现存在错误，直接返回错误信息，另外如果存在这种错误，可以采用异常的处理方式，抛出异常，继续执行其他可以识别的列名

1.我认为当前的设计应该不会出现大量的重复代码，因为我这边读取的数据集都是从configurationservice中读取的配置，我只需要在配置类中配置相应的读取函数就可
2.对于load_raw_data，目前是只对其pd.DataFrame,如果说需要与配置解耦，我认为需要判断传入的配置文件的类型，然后传入的时候转换成统一的格式，例如说全部转换字典、pd.DataFrame等等


