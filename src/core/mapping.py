import os
import sys
import logging
from pathlib import Path

# from src.utils.excel_to_yaml import excel_to_yaml

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.append(str(project_root))

# 现在可以使用绝对导入了
from src.utils.excel_to_yaml import excel_to_yaml


def generate_mapping() -> None:
    ''' 批量生成映射配置文件'''
    # 设置路径
    design_dir = Path('docs/design')
    output_dir = Path('config/mapping')

    # 确保输出路径存在
    output_dir.mkdir(parents=True, exist_ok=True)

    # 计数器
    success_count = 0
    error_count = 0

    # 遍历设计文档目录下的所有Excel文件
    for excel_file in design_dir.glob('*.xlsx'):
        try:
            # 生成输出文件名
            output_file = output_dir / f'{excel_file.stem}.yaml'

            # 调用转换函数
            excel_to_yaml(
                excel_path=str(excel_file),
                yaml_path=str(output_file)
            )
            logging.info(f'Generated mapping file: {output_file}')
            success_count += 1
        except Exception as e:
            logging.error(f'Error processing {excel_file}: {e}')
            error_count += 1

    logging.info(f'Processing complete: {success_count} succeeded, {error_count} failed')


if __name__ == '__main__':
    generate_mapping()
