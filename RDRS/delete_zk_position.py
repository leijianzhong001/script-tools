# -*- coding:utf-8 -*-
from kazoo.client import KazooClient
import sys
import argparse

# 使用argparse模块解析命令行参数
parser = argparse.ArgumentParser(description='delete zk position node')
parser.add_argument("-t", "--task_id", help="task id", required=True)
parser.add_argument("-i", "--zk_ip", help="zk ip", default='10.93.1.87')

args = parser.parse_args()
print('args', args)

# 从命令行中接收task_id参数
task_id = args.task_id
# 如果zk_ip参数为空，则使用默认值
zk_ip = args.zk_ip

# 查看节点信息
task_id = task_id.strip()
if not task_id:
    print('task_id is empty, exit.')
    sys.exit()

# 创建KazooClient对象并连接ZooKeeper服务器
zk = KazooClient(hosts=f'{zk_ip}:2181')
zk.start()


status_note_path = f'/dtsf/tasks/{task_id}/status'
# 检查节点是否存在
if not zk.exists(status_note_path):
    print(f'task {task_id} not exist status node {status_note_path}, exit.')
    zk.stop()
    sys.exit()

# 获取指定任务的status节点的数据
status_data, stat = zk.get(status_note_path)
# 使用 iso-8859-1 解码，否则会报错
status_content = status_data.decode('latin-1')
# 检查status_content中是否包含"master has purged binary logs"关键字
print(f'task {task_id} status_content:')
print(status_content)
print()
if 'master has purged binary logs' not in status_content:
    print(f'task {task_id} is not purged binary logs, exit.')
    zk.stop()
    sys.exit()

# 如果存在position节点，则删除指定任务的position节点
position_note_path = f'/dtsf/tasks/{task_id}/position'
if zk.exists(position_note_path):
    zk.delete(position_note_path)
    print(f'delete task {task_id} position node {position_note_path} success.')
else:
    print(f'task {task_id} not exist position node {position_note_path}, exit.')

# 关闭连接
zk.stop()
