class FileUtils:
    # def __init__(self) -> None:
    #     self.project_name = parse_command_line().project
    
    # NOTE 目前的项目暂时不需要去运行多个项目,就先基于本项目目录开始
    def get_raw_data_path(self, config_file: str | None, path: str) -> str:
        datafile = '%s%s' %(path, config_file)
        return '0'
    
    def get_all_raw_data_path(self):
        pass
    
    def read_csv_to_pd(self):
        pass
    
    def save_df_by_timestamp(self):
        pass
    
class DBUtils:
    def create_model_class(self):
        pass
    
    def execute_raw_data_import_sqlite(self):
        pass
    
    def read_spec_df(self):
        pass
    
    def arrange_out_df(self):
        pass
    
class ConfigUtils:
    @staticmethod
    def get_whole_config():
        pass
    
    @staticmethod
    def get_exe_ls():
        pass

    
import pandas as pd 
import os
from typing import cast

class DataReady:
    def __init__(self, file_utils: FileUtils, db_utils: DBUtils) -> None:
        self.file_utils = file_utils
        self.db_utils = db_utils
        
    def read_raw_df(self) -> pd.DataFrame:
        df = pd.DataFrame()
        return df
    
    def read_spec_df(self):
        pass
        
    def fetch_norm_table(self, norm: pd.DataFrame) -> pd.DataFrame:
        ds_id: str = norm['程序中数据集ID'].iloc[0]
        subjid: str = norm['subjid'].iloc[0]
        source_table: str = norm['来源表'].iloc[0]
        source_field: list[str] = norm['来源变量'].tolist()
        # NOTE 这个地方用的loc,返回的是一个dataframe,所以where_condition是一个dateframe
        where_condition: pd.DataFrame = norm.loc[norm['where语句']!='']
        where_str = ''
        # NOTE .shape返回的是一个元组(行数, 列数),因此.shape[0]取的是行数
        if where_condition.shape[0] != 0:
            where_ls: list[str] = []
            for idx, row in where_condition.iterrows():
                col_name: str = row['来源表变量']
                where_comment: str = row['where语句']
                # NOTE 例如配置文件中where列写明{$} > 18, col_name = age
                # NOTE 这个地方就可以将{$} 替换为 age, 就是 age > 18
                if '{$}' in where_comment:
                    where_comment = where_comment.replace('{$}', col_name)
                where_ls.append(f'({where_comment})')
                # NOTE 将列表用 or 拼接起来
                where_str = ' or '.join(where_ls)
                where_str = f'where ({where_str})'
        raw_path: str = self.file_utils.get_raw_data_path(f'{ds_id}.csv', path='raw/test_store')
        if not os.path.exists(raw_path):
            df = self.read_raw_df()
            df.to_csv(raw_path, index = False)
        else:
            df = pd.read_csv(raw_path)
        return df
    
    def fetch_dict_table(self, dict_df: pd.DataFrame) -> pd.DataFrame:
        where_str = ''
        where_condition: pd.DataFrame = dict_df.loc[dict_df['where语句']!='']
        if where_condition.shape[0] != 0:
            where_ls: list = []
            for idx, row in where_condition.iterrows():
                col_name: str = row['来源表变量']
                where_comment: str = row['where语句']
                # NOTE 例如配置文件中where列写明{$} > 18, col_name = age
                # NOTE 这个地方就可以将{$} 替换为 age, 就是 age > 18
                if '{$}' in where_comment:
                    where_comment = where_comment.replace('{$}', col_name)
                where_ls.append(f'({where_comment})')
                # NOTE 将列表用 or 拼接起来
                where_str = ' or '.join(where_ls)
                where_str = f'where ({where_str})'
        ds_id: str = dict_df['程序中数据集ID'].iloc[0]
        subjid: str = dict_df['subjid'].iloc[0]
        source_table: str = dict_df['来源表'].iloc[0]
        source_field: list[str] = dict_df['来源变量'].tolist()
        select_str = f'select {",".join(source_field)} from raw_df {where_str}'
        df = self.read_raw_df()
        df = df.reindex(columns= source_field)
        df.fillna('', inplace= True)
        df = df.astype(str)
        df = df.replace('NaT','')
        return df

    
    def prepare_raw_data(self, config_file: str | None = None) -> dict[str, pd.DataFrame]:
        # NOTE 获取dm集配置文件所在地
        config_path = self.file_utils.get_raw_data_path(config_file= config_file, path= 'config')
        config_df = pd.read_excel(config_path, dtype= object, keep_default_na= False).fillna('')
        # NOTE 对dm集配置文件中的字典表与其他表进行切分
        dict_df = config_df.loc[config_df['处理方式'] == '字典']
        norm_df = config_df.loc[config_df['处理方式'].isin(['常规', ''])]
        norm_series = norm_df.groupby(['程序中数据集ID']).apply(lambda x: self.fetch_norm_table(x))
        # raw_data_dic: dict[str, pd.DataFrame] = {str(k): v for k, v in result_series.to_dict().items()}
        # raw_data_dic: dict[str, pd.DataFrame] = result_series.to_dict()
        # NOTE 之前一直报错的原因是pandas的to_dict()方法前面是泛型的，返回的是dict[Hashable, Any],检查器看到的Series.todict() -> dict[Hashable, Any]
        # NOTE 和我想要赋值的dict[str, pd.DataFrame]不符，因此需要进行转换
        raw_data_dic:dict[str, pd.DataFrame] = cast(dict[str, pd.DataFrame], norm_series.to_dict())

        if not dict_df.empty:
            dict_series = dict_df.groupby(['程序中数据集ID']).apply(lambda x: self.fetch_dict_table(x))
        dict_data:dict[str, pd.DataFrame] = cast(dict[str, pd.DataFrame], dict_series.to_dict())
        # NOTE 合并两个字典
        raw_data_dic.update(dict_data)
        
        return raw_data_dic
        
        
####################################################################################################################

class DM:
    def __init__(self, dataready: DataReady) -> None:
        self.dataready = dataready
    
    def run_dm_ds(self):
        raw_data_store: dict[str, pd.DataFrame] = self.dataready.prepare_raw_data(config_file= 'dm_rawdata_config.xlsx')
        self.core_task(raw_data_store)
        
    def core_task(self, raw_data_store: dict[str, pd.DataFrame]):
        spec_df = self.dataready.read_spec_df()
        dm1_df: pd.DataFrame = raw_data_store['dm1']
        dm2_df: pd.DataFrame = raw_data_store['dm2']
        dm3_df: pd.DataFrame = raw_data_store['dm3']
        pass
    
    def map_sex(self):
        pass
    
    def map_brthdtc(self):
        pass
    
    def map_height_weight(self):
        pass
    
    def calculate_bmi(self):
        pass
    


class DataProcessor:
    def __init__(self, file_utils: FileUtils, db_utils: DBUtils, dataready:DataReady) -> None:
        self.file_utils = file_utils
        self.db_utils = db_utils
        self.dataready = dataready
    
    def run_dm(self):
        dm = DM(self.dataready)
        dm.run_dm_ds()
        self.save_result()
    
    def save_result(self):
        self.file_utils.save_df_by_timestamp()
    
class TaskManager():
    def __init__(self, config: ConfigUtils) -> None:
        self.config = config
    
    def run_ds_multi_process(self):
        tasks = self.config.get_exe_ls()
    
    def process_main_task_ds(self):
        pass
    
    def combine_bath_df(self):
        pass
    

####################################################################################################################
class MainApp:
    @staticmethod
    def run_project():
        file_utils = FileUtils()
        db_utils = DBUtils()
        dataready = DataReady(file_utils, db_utils)
        config_utils = ConfigUtils()
        data_processor = DataProcessor(file_utils, db_utils, dataready)
        data_processor.run_dm()

    def run_df_process(self):
        config_utils = ConfigUtils()
        taskmanager = TaskManager(config_utils)
        taskmanager.run_ds_multi_process()

class ProcessManager:
    @staticmethod
    def main(project_name: str):
        MainApp.run_project()

# import getopt
# import sys

# def command_line():
#     project_name = None
#     para = {}
#     try:
#         opts, args = getopt.getopt(
#             sys.argv[1:],
#             shortopts= 'hcaeqtbmrs',
#             longopts= ["project=",'save_resolved_query','generate_query_work','export_query_report'])
#         for opt, arg in opts:
#             if opt == '-h':
#                 sys.exit(1)
#             elif opt == '--project':
#                 project_name = arg
#             else: 
#                 para[opt] = True
#     except getopt.GetoptError:
#         print('getopt error')
#         sys.exit(1)
#     finally:
#         if project_name:
#             print('待执行项目为%s' %project_name)
#             return project_name, para        
'''
举例
python my_script.py --project my_project -c -a
输出: 待执行项目为 my_project
返回值：
project_name : 'my_proejct'
para : {'-c': ['非选项参数咧白哦'], '-a': ['非选项参数列表']}
'''



import argparse
import sys

def parse_command_line() -> argparse.Namespace:
    '''
    使用argparse解析命令行参数
    返回解析后的参数命名空间
    '''
    # todo 创建解析器对象
    parser = argparse.ArgumentParser(
        description= '命令行参数解析示例',
        epilog= '示例方法: python script.py --project myproject --save_resolved_query --generate_query_work'
    )
    
    # todo 添加项目参数(必需参数)
    parser.add_argument('--project', '-p', required= True, help= '指定要执行的项目名称')

    # todo 添加各种功能标志参数
    parser.add_argument('--save_resolved_query', '-s', action= 'store_true', help= '保存已解析的查询')
    parser.add_argument('--generate_query_work', '-g', action= 'store_true', help= '生成查询工作')
    parser.add_argument('--export_query_report', '-e', action= 'store_true', help= '导出查询报告')

    # todo 添加其他短选项标志
    parser.add_argument('-c', action= 'store_true', help= '选项c')
    parser.add_argument('-a', action= 'store_true', help= '选项a')
    parser.add_argument('-q', action= 'store_true', help= '选项q')
    parser.add_argument('-t', action= 'store_true', help= '选项t')
    parser.add_argument('-b', action= 'store_true', help= '选项b')
    parser.add_argument('-m', action= 'store_true', help= '选项m')
    parser.add_argument('-r', action= 'store_true', help= '选项r')
    # 注意: -h 由 argparse 自动处理
    
    # todo 解析参数
    args: argparse.Namespace = parser.parse_args()
    
    # 这里是args.project,project是自动生成的
    print(f'待执行项目为:{args.project}')
    return args
    
if __name__ == '__main__':
    # try:
    #     args: argparse.Namespace = parse_command_line()
    #     # vars是将argparse的命名空间对象转换成字典
    #     print('解析的参数:', vars(args))

    #     # 使用参数示例
    #     if args.save_resolved_query:
    #         print('执行保存已解析查询操作...')
    #     if args.generate_query_work:
    #         print("执行生成查询工作操作...")
    # except SystemExit:
    #     pass
    args = parse_command_line()
    project = args.project
    ProcessManager.main(project_name= project)