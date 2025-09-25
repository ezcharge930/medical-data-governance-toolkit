
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