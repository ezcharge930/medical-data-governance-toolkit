import pandas as pd 
import os
from typing import cast
from ..utils.file_utils import FileUtils
from ..utils.db_utils import DBUtils
from ..utils.config_utils import ConfigUtils
# from ..domain.dm import DM

class DataReady:
    def __init__(self, file_utils: FileUtils, db_utils: DBUtils, whole_config : dict | None = None) -> None:
        self.file_utils = file_utils
        self.db_utils = db_utils
        self.whole_config = whole_config or {}
        
    def read_raw_df(self, table, where, source_path) -> pd.DataFrame:
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
            df = self.read_raw_df(table= norm, where= where_str, source_path= source_table)
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
        df = self.read_raw_df(table= dict_df, where= where_str, source_path= source_table)
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
    