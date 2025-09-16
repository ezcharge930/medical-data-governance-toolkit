import pandas as pd

# 配置类
class ConfigManager:
    # 获取源数据地址
    def get_raw_data_path(self, config_path, path):
        pass
   
# 样本抽样类 
class SampleManager:
    # 获取随机患者
    def prepare_random_sample(self, sample_file):
        pass

# 数据治理类
class DataValidator:
    pass

# 源数据处理
class RawDataProcessor:
    def __init__(self, configmanager: ConfigManager, samplemanager: SampleManager, datavalidator: DataValidator) -> None:
        self.configmanager = configmanager
        self.samplemanager = samplemanager
        
    # 常规数据治理
    def calculate_dict_grp(self):
        pass

    # 字典数据治理
    def calculate_com_grp(self):
        pass


    # 获取原始数据
    def prepare_raw_date(self,is_test= False, batch_pt_ls= [], config_file= None, sample_file= 'sample_subj_ls.xlsx', whole_config= {}):
        is_raw_data_in_db: bool = False
        if whole_config.get('是否使用数据库','').lower() == 'y':
            is_raw_data_in_db: bool = True
            print('数据库连接')
            
        #todo 获取config_path
        config_path = self.configmanager.get_raw_data_path(config_file, path= 'config')
        
        #todo 获取配置表
        config_df = pd.read_excel(config_file, dtype=object, keep_default_na= False).fillna('')

        raw_data_dict = {}

        #todo 将字典的表提取出来
        dict_df = config_df.loc[config_df['处理方式'] == '字典']
        
        #todo 把非字典表提取出来
        com_df = config_df.loc[config_df['处理方式'].isin(['常规',''])]
        
        #todo 随机患者
        test_subjid_list = self.samplemanager.prepare_random_sample(sample_file)
        
        if is_test: # 测试模式下
            batch_pt_ls = test_subjid_list
        if batch_pt_ls is None: # 传入的batch_pt_ls如果是空的情况下
            batch_pt_ls = []
        batch_pt_ls = [f"'{i}'" for i in batch_pt_ls]
        
        com_grp = self.calculate_com_grp()
        
        dict_grp = self.calculate_dict_grp()
        
class ReadRawData:
    def __init__(self, configmanager: ConfigManager) -> None:
        self.configmanager = configmanager
        
    def read_spec_df(self, domain, spec_file=''):
        spec_path = self.configmanager.get_raw_data_path(spec_file, path='config')
        spec_df = pd.read_excel(spec_path, dtype= object, sheet_name= '计算规则', keep_default_na= False)
        spec_df = spec_df.loc[(spec_df['数据集'] == domain) & (spec_df['输出标记'].str.lower() == 'y')]

        return spec_df
        

    
    
    
    