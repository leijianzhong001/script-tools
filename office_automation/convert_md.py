# -*- coding:utf-8 -*-

import os

rootDir = 'D:\\file\\PycharmProjects\\'


def read_py_file(file_name):
    """
    Read python file and return list of lines
    :param file_name:
    :return:
    """
    with open(file_name, 'r', encoding='utf-8') as py_file_obj:
        lines = py_file_obj.readlines()
        return lines


for dir_path, dir_names, files in os.walk(r'D:\BaiduSyncdisk\temp\python', topdown=False):
    for py_file_name in files:
        if not py_file_name.endswith('.md') and not py_file_name.endswith('.py'):
            continue

        py_file_path = os.path.join(dir_path, py_file_name)
        if py_file_path.find('lesson_') == -1:
            continue

        # 从.py文件中读取到的行列表
        py_file_lines = read_py_file(py_file_path)

        # 获得课程文件夹名称
        path_list = py_file_path.split(os.sep)
        lesson_dir_name = rootDir + path_list[4]

        # 如果文件夹没有，创建文件夹
        if not os.path.exists(lesson_dir_name):
            # 创建一个文件夹
            os.makedirs(lesson_dir_name)
            print(f'create dir {lesson_dir_name} successfully!')

        # .md文件名称
        md_file_name = py_file_name.replace('.md', '').replace('.py', '')
        lesson_file_name = lesson_dir_name + os.sep + md_file_name + '.md'
        with open(lesson_file_name, 'w', encoding='utf-8') as md_file_obj:
            if py_file_name.endswith('.md'):
                # 如果原始文件就是.md, 直接写入所有的行
                md_file_obj.writelines(py_file_lines)
            if py_file_name.endswith('.py'):
                # 如果原始文件是.py, 需要加上代码段标签
                md_file_obj.write('```python\r\n')
                md_file_obj.writelines(py_file_lines)
                md_file_obj.write('```\r\n')
            print(f'{py_file_path} successfully converted!')

