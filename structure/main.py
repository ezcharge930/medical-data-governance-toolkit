# import argparse
# import sys
# import os
# from structure.utils.file_utils import FileUtils
# from structure.utils.db_utils import DBUtils
# from structure.utils.config_utils import ConfigUtils
# from structure.data.raw_data import DataReady
# from structure.data.data_processor import DataProcessor
# from structure.data.task_manager import TaskManager


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
        
# class ProcessManager:
#     @staticmethod
#     def main(project_name: str):
#         MainApp.run_project()
    

# def parse_command_line() -> argparse.Namespace:
#     '''
#     使用argparse解析命令行参数
#     返回解析后的参数命名空间
#     '''
#     # NOTE 创建解析器对象
#     parser = argparse.ArgumentParser(
#         description= '命令行参数解析示例',
#         epilog= '示例方法: python script.py --project myproject --save_resolved_query --generate_query_work'
#     )
    
#     # NOTE 添加项目参数(必需参数)
#     parser.add_argument('--project', '-p', required= True, help= '指定要执行的项目名称')

#     # NOTE 添加各种功能标志参数
#     parser.add_argument('--save_resolved_query', '-s', action= 'store_true', help= '保存已解析的查询')
#     parser.add_argument('--generate_query_work', '-g', action= 'store_true', help= '生成查询工作')
#     parser.add_argument('--export_query_report', '-e', action= 'store_true', help= '导出查询报告')

#     # NOTE 添加其他短选项标志(可选功能,未来可能会用到)
#     parser.add_argument('-c', action= 'store_true', help= '选项c')
#     parser.add_argument('-a', action= 'store_true', help= '选项a')
#     parser.add_argument('-q', action= 'store_true', help= '选项q')
#     parser.add_argument('-t', action= 'store_true', help= '选项t')
#     parser.add_argument('-b', action= 'store_true', help= '选项b')
#     parser.add_argument('-m', action= 'store_true', help= '选项m')
#     parser.add_argument('-r', action= 'store_true', help= '选项r')
#     # 注意: -h 由 argparse 自动处理
    
#     # todo 解析参数
#     # args: argparse.Namespace = parser.parse_args()
    
#     # # 这里是args.project,project是自动生成的
#     # print(f'待执行项目为:{args.project}')
#     return parser.parse_args()
import sys
import os
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"🔧 已强制修复路径: {project_root}")
from structure.cli.runner import ApplicationRunner

def main():
    # try:
    #     args = parse_command_line()
    #     print(f'待执行项目为: {args.project}')
    #     print('解析的参数:', vars(args))
        
    #     # 根据命令行参数执行不同的流程
    #     if args.save_resolved_query or args.generate_query_work or args.export_query_report:
    #         MainApp.run_project()
    #     else:
    #         print('未指定具体操作,请提供至少一个功能标志,如: -s, -g 或 -e')
    #         sys.exit(1)
    #     # 若有其他并行任务处理需求
    #     if any([args.c, args.a, args.q, args.t, args.b, args.m, args.r]):
    #         MainApp.run_df_process()
            
    # except SystemExit:
    #     pass
    # except Exception as e:
    #     print(f'程序运行出错:{e}')
    #     sys.exit(1)
    runner = ApplicationRunner()
    runner.run()


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
    # args = parse_command_line()
    # project = args.project
    # ProcessManager.main(project_name= project)
    main()
    
