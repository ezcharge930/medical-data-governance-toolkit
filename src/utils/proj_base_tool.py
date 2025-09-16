import pandas as pd
import getopt
import sys
import importlib


def main(project_dir: str, para_dict: dict | None = None):
    """ process a project report """
    import_str = 'project.%s' % project_dir
    module = importlib.import_module(import_str)
    run_project = getattr(module, 'run_project')
    if para:
        run_project(project_dir, para_dict)
    else:
        run_project(project_dir)


# 命令行参数解析函数
def command_line():
    p = None  # 初始化项目名称:None
    _para = {}  # 初始化参数字典

    try:
        # todo 解析命令行参数
        opts, args = getopt.getopt(
            sys.argv[1:],  # 获取除脚本名外的所有参数
            'hcaeqtbmrs',  # 支持的短选项字符串
            [  # 支持的长选项字符串
                'project=',  # 需要参数的选项
                'save_resolved_query',  # 无参数选项
                'generate_query_work',
                'export_query_report',
            ]
        )

        # 遍历解析到的结果
        for opt, arg in opts:
            if opt == '-h':  # 帮助程序
                sys.exit(1)  # 退出程序
            elif opt in '--project':  # 项目名称选项
                p = arg  # 保存项目名称
            else:  # 其他选项
                _para[opt] = args

    except getopt.GetoptError:
        print('get opt error!')
        sys.exit(2)

    finally:
        if p:
            print('待执行项目为：%s' % p)
            return p, _para
        else:
            return None, _para


project, para = command_line()

if __name__ == '__main__':
    if project:
        if para != {}:
            main(project, para)
        else:
            main(project)


# 配置类
class ConfigManager:
    # 获取源数据地址
    @staticmethod
    def get_raw_data_path(data_file: str, path: str = 'raw') -> str:
        return 'project/%s/%s/%s' % (project, path, data_file)

    def get_whole_config(self, config_file: str = 'whole_config.xlsx'):
        # todo 
        self.get_raw_data_path(config_file, path='config')


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
    def __init__(self, configmanager: ConfigManager, samplemanager: SampleManager,
                 datavalidator: DataValidator) -> None:
        self.configmanager = configmanager
        self.samplemanager = samplemanager

    # 常规数据治理
    def calculate_dict_grp(self):
        pass

    # 字典数据治理
    def calculate_com_grp(self):
        pass

    # 获取原始数据

    def prepare_raw_date(self, is_test=False, batch_pt_ls: list | None = None, config_file: str | None = None,
                         sample_file='sample_subj_ls.xlsx',
                         whole_config: dict | None = None, test_subside_list=None):
        if whole_config is None:
            whole_config = {}
        if batch_pt_ls is None:
            batch_pt_ls = []
        is_raw_data_in_db: bool = False
        if whole_config.get('是否使用数据库', '').lower() == 'y':
            is_raw_data_in_db: bool = True
            print('数据库连接')

        # todo 获取config_path
        config_file_str = config_file if config_file is not None else 'whole_config.xlsx'
        self.configmanager.get_raw_data_path(config_file_str, path='config')

        # todo 获取配置表
        config_df = pd.read_excel(config_file, dtype=object, keep_default_na=False).fillna('')

        raw_data_dict = {}

        # todo 将字典的表提取出来
        dict_df = config_df.loc[config_df['处理方式'] == '字典']

        # todo 把非字典表提取出来
        com_df = config_df.loc[config_df['处理方式'].isin(['常规', ''])]

        # todo 随机患者
        self.samplemanager.prepare_random_sample(sample_file)

        if is_test:  # 测试模式下
            batch_pt_ls = test_subside_list
        if batch_pt_ls is None:  # 传入的batch_pt_ls如果是空的情况下
            batch_pt_ls = []
        batch_pt_ls = [f"'{i}'" for i in batch_pt_ls]

        self.calculate_com_grp()

        self.calculate_dict_grp()


class ReadRawData:
    def __init__(self, configmanager: ConfigManager) -> None:
        self.configmanager = configmanager

    def read_spec_df(self, domain, spec_file=''):
        spec_path = self.configmanager.get_raw_data_path(spec_file, path='config')
        spec_df = pd.read_excel(spec_path, dtype=object, sheet_name='计算规则', keep_default_na=False)
        spec_df = spec_df.loc[(spec_df['数据集'] == domain) & (spec_df['输出标记'].str.lower() == 'y')]

        return spec_df
