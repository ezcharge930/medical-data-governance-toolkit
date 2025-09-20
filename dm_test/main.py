import argparse
import sys
from process import ProcessManager

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