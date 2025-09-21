
from structure.cli.parser import ArgumentParser
from structure.core.app import Application
from structure.utils.config_utils import ConfigUtils

class ApplicationRunner:
    ''' #NOTE 应用执行器'''
    def run(self):
        ''' #NOTE 运行应用'''
        try:
            # 解析参数
            args = ArgumentParser.parse_command_line()
            print(f'启动项目:{args.project}')
            
            # 创建应用实例
            app = Application(args.project, ConfigUtils())
            
            # 根据参数执行不同的功能
            self._execute_main_task(app= app, args= args)
            self._execute_optional_tasks(app= app, args= args)
            
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
        
    def _execute_optional_tasks(self, app: Application, args):
        ''' 执行可选任务'''
        if any([args.c, args.a, args.t]):
            app.run_parallel_tasks()