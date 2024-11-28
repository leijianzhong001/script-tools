# -*- coding:utf-8 -*-
import argparse
import os
import subprocess
import sys


def check_user(username):
    try:
        subprocess.run(['id', username], check=True, timeout=5)
        return True
    except subprocess.CalledProcessError:
        return False


def create_user(username):
    subprocess.run(['sudo', 'useradd', username])
    subprocess.run(['sudo', 'echo', f'{username}:Rdrs2023', '|', 'sudo', 'chpasswd'])  # 设置密码


def check_group(group_name):
    try:
        subprocess.run(['sudo', 'getent', 'group', group_name], check=True, timeout=5)
        return True
    except subprocess.CalledProcessError:
        return False


def create_group(group_name):
    subprocess.run(['sudo', 'groupadd', group_name], timeout=5)


def check_user_group(username, group_name):
    try:
        # 获取指定用户的组列表
        output = subprocess.check_output(['id', '-nG', username], universal_newlines=True, timeout=5)
        groups = output.strip().split()
        return group_name in groups
    except subprocess.CalledProcessError:
        return False


def add_user_to_group(username, group_name):
    subprocess.run(['sudo', 'usermod', '-a', '-G', group_name, username], timeout=5)


def check_sudo_privilege(username):
    try:
        output = subprocess.check_output(['sudo', '-l', '-U', username], stderr=subprocess.DEVNULL,
                                         universal_newlines=True)
        if 'may run the following commands' in output:
            return True
    except subprocess.CalledProcessError:
        pass
    return False


def add_to_admin_group(username):
    subprocess.run(['sudo', 'usermod', '-aG', 'sudo', username])


def add_sudo_privilege(username):
    sudoers_file = '/etc/sudoers'

    # 检查是否已经有sudoers文件的写入权限
    if not check_sudoers_file_permission():
        print("unable to modify the sudoers file. Make sure you have adequate permissions.")
        return

    # 将用户添加到sudoers文件中
    with open(sudoers_file, 'a') as file:
        file.write(f"{username} ALL=(ALL)  NOPASSWD: ALL\n")
    print(f"user {username} add to admin group successfully！")


def check_sudoers_file_permission():
    try:
        # 通过执行sudo -v命令，用户可以保持其sudo权限的活动状态，而无需实际执行任何命令。这对于在一段时间内需要保持sudo权限的情况很有用，例如在执行多个sudo操作之间的间隔时间。
        # 实际使用该命令时，如果用户没有sudo权限会返回非零值，这里我们忽略输出，只关心是否有异常
        subprocess.run(['sudo', '-v'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)
        return True
    except subprocess.CalledProcessError:
        return False


def cmd_args():
    """
    接收命令行参数
    :return:
    """
    parser = argparse.ArgumentParser(
        description='This script is used to install the standard environment for worker nodes, '
                    'including user creation and Java Runtime installation')  # 使用argparse的构造函数来创建对象
    parser.add_argument("-u", "--user", help="worker user name", required=True)  # 添加可解析的参数
    parser.add_argument("-g", "--group", help="worker user group")  # 添加可解析的参数
    args = parser.parse_args()
    print(' OK: cmd args: {}'.format(args))

    if args.group is None and args.user is not None:
        args.group = args.user

    if args.user is None:
        args.user = 'dtsfsys'

    if args.group is None:
        args.group = 'dtsfsys'

    return args.user, args.group


def check_user_java_permission(username):
    try:
        path = f'/home/{username}/.bashrc'
        with open(path, 'r') as bashrc_file:
            for line in bashrc_file:
                if 'JAVA_HOME' in line:
                    return 1
        return 0
    except FileNotFoundError:
        return 2


def check_keyword_in_file(file_path, keyword):
    try:
        with open(file_path, 'r') as file:
            contents = file.read()
            if keyword in contents:
                return True
            else:
                return False
    except FileNotFoundError:
        print(f"The file '{file_path}' does not exist.")
        return False


# 从命令行参数中获取用户和组名
target_user_name, target_group_name = cmd_args()

# 检查并创建用户
if not check_user(target_user_name):
    create_user(target_user_name)
    print(f"user {target_user_name} create successfully！")
else:
    print(f"user {target_user_name} already exists！")

# 检查并创建组
if not check_group(target_group_name):
    create_group(target_group_name)
    print(f"group {target_group_name} create successfully！")
else:
    print(f"group {target_group_name} already exists！")

# 检查用户是否属于指定组
if not check_user_group(target_user_name, target_group_name):
    add_user_to_group(target_user_name, target_group_name)
    print(f"user {target_user_name} add to {target_group_name} group successfully！")
else:
    print(f"user {target_user_name} already belong to group {target_group_name}！")

# 授予用户sudo权限
if not check_sudo_privilege(target_user_name):
    add_sudo_privilege(target_user_name)
else:
    print(f"user {target_user_name} already belong the administrator group！")

# 安装Java Runtime
jdk_tz_file = 'openjdk.tar'
jdk_dir_name = 'openjdk-1.8.0_92'
unzip_dir = f'/usr/local/{jdk_dir_name}'

# 1. 判断java运行时环境是否就绪，如果就绪，退出安装
result = check_user_java_permission(target_user_name)
if result == 1:
    print('Java runtime environment is already installed. Exiting installation.')
    sys.exit()
if result == 2:
    print('The user does not exist. Exiting installation.')
    sys.exit()

# 2. 解压安装包到/usr/local/目录
# 检查是否已经存在 f'/usr/local/{jdk_dir_name}' 目录
if os.path.exists(unzip_dir):
    print(f"The directory {unzip_dir} exists.")
    sys.exit()

# 不存在则解压安装包
subprocess.run(['tar', 'xf', f'/tmp/{jdk_tz_file}', '-C', '/usr/local/'], check=True)
print('unzip jdk successfully！')

# 3. 修改解压后的目录及其子目录的用户和用户组为dtsfsys
subprocess.run(['chown', '-R', 'dtsfsys:dtsfsys', unzip_dir], check=True)
print('change jdk owner successfully！')

# 4. 配置jdk环境变量到/etc/profile中
exists = check_keyword_in_file(f'/home/{target_user_name}/.bashrc', 'JAVA_HOME')
if exists:
    print(f'JAVA_HOME already exists in /home/{target_user_name}/.bashrc！')
    sys.exit()

java_home_line = f'export JAVA_HOME={unzip_dir}\n'
java_path_line = 'export PATH=$JAVA_HOME/bin:$PATH\n'
with open(f'/home/{target_user_name}/.bashrc', 'a') as profile_file:
    profile_file.write(java_home_line)
    profile_file.write(java_path_line)
print('configure jdk environment successfully！')

# 5. 生效环境变量 /home/dtsfsys/.bashrc 这里注释掉，不指定source命令，因为sudo source会报command not found
# subprocess.run(['source', f'/home/{target_user_name}/.bashrc'], check=True)
# print('source jdk environment successfully！')
