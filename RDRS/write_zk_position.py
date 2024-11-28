# -*- coding:utf-8 -*-
# -*- coding:utf-8 -*-
from kazoo.client import KazooClient
import argparse
import sys

# 使用argparse模块解析命令行参数
parser = argparse.ArgumentParser(description='Set zk position node')
parser.add_argument("-t", "--task_id", help="task id", required=True)
parser.add_argument("-p", "--position", help="position content", required=True)

args = parser.parse_args()

# 从命令行中接收task_id和position参数
task_id = str(args.task_id).strip()
position = args.position

# 创建KazooClient对象并连接ZooKeeper服务器
zk = KazooClient(hosts='10.93.1.87:2181')
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
if 'master has purged binary logs' not in status_content:
    print(f'task {task_id} is not purged binary logs, exit.')
    zk.stop()
    sys.exit()


# 设置position节点的内容
position_note_path = f'/dtsf/tasks/{task_id}/position'
if zk.exists(position_note_path):
    zk.set(position_note_path, position.encode('utf-8'))
    print(f'set task {task_id} position node {position_note_path} success.')
else:
    print(f'task {task_id} not exist position node {position_note_path}, exit.')

## status_content的内容是一个json字符串，将其解析为字典

# 关闭连接
zk.stop()
