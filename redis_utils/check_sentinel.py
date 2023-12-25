# -*- coding:utf-8 -*-

import subprocess
import sys
import argparse
import re
import json
import urllib2

# =========================================>>> 用途描述 <<<=========================================
SCRIPT_DOCS = """
这个脚本用来检查sentinel迁移完成之后，sentinel当前集群状态是否正常，主要检查如下项目：
    1、所有的sentinel节点是否正确运行（是否可以ping通）
    2、所有的sentinel节点是否处于tilt模式，如果进入tilt模式，则认为sentinel状态不正常
    3、随机挑选100个shard，检查其master字典中sentinel标志的数量是否为 SENTINEL_NUM_CORRECT = 3
    4、随机挑选100个shard，检查其sentinel字典中迁移的sentinel节点的flags标志是否为 OK_FLAGS = 'sentinel'
    5、随机挑选100个shard，检查其sentinel字典中迁移的sentinel节点的 last_ok_pint_reply 时间是否小于 MAX_LAST_OK_PING_REPLY = 2000
    6、随机挑选100个shard，检查其sentinel字典中迁移的sentinel节点的 last_hello_message 时间是否小于 MAX_LAST_HELLO_MESSAGE = 4000
脚本命令格式：
    python check_sentinel.py -t <sentinel_ip>
    其中sentinel_ip就是迁移的sentinel节点
"""
# =========================================>>> 用途描述 <<<=========================================


# =========================================>>> 常量定义 <<<=========================================

# 如果当前sentinel认为待检测的目标sentinel节点是一个sentinel，并且状态正常，则master的sentinels字典中的flags的值为 sentinel
OK_FLAGS = 'sentinel'
# ping消息是1s一次，所以这里最大阈值定义为 2s, 如果超过2s，说明当前sentinel和待检测的目标sentinel ping消息超时，认为检测失败
MAX_LAST_OK_PING_REPLY = 2000
# hello消息是2s发一次，所以这里最大阈值定义为4s，如果 4s 内没有收到指定sentinel的hello消息，则认为该sentinel异常
MAX_LAST_HELLO_MESSAGE = 4000

CMD_T_INFO_SENTINEL = ['redis-cli', '-h', '', '-p', '26379', 'info', 'sentinel']
CMD_T_SENTINEL_SENTINELS = ['redis-cli', '-h', '', '-p', '26379', 'sentinel', 'sentinels', '']
CMD_T_PING = ['redis-cli', '-h', '', '-p', '26379', 'ping']
# 查询sentinel集群信息, 返回结果：10.237.188.57,10.237.188.58,10.237.188.6
SQL_T_QUERY_SENTINEL_CLUSTER = "SELECT GROUP_CONCAT(ip) FROM domain_and_ip WHERE id = (SELECT id FROM domain_and_ip WHERE ip='{}' ORDER BY id LIMIT 1) AND is_deleted=0"

# 模式 normal为正式运行模式，test为测试模式, 测试模式不会尝试去做太严格的检查，比如去ping sentinel节点
MODE = 'normal'
# 正确的sentinel数量
SENTINEL_NUM_CORRECT = 3
# 0 表示退出tilt模式；1 表示进入tilt模式
TILT_MODE = '0'

# =========================================>>> 常量定义 <<<=========================================


class MasterInfo:
    """
    info sentinel 返回的条目，格式如下：
    master0:name=CPS_SIT_1,status=ok,address=10.243.147.126:6379,slaves=1,sentinels=3
    """
    def __init__(self):
        self.master_idx = None
        self.name = None
        self.status = None
        self.address = None
        self.slaves = None
        self.sentinels = None

    def parse(self, info_sentinel_entry):
        """
        从字符串中解析出 MasterInfo 对象
        :param info_sentinel_entry:
        :return:
        """
        if info_sentinel_entry is None or len(info_sentinel_entry) == 0:
            return self
        items = info_sentinel_entry.split(',')
        for item in items:
            if item.startswith('master'):
                self.master_idx = item.split(':')[0]
                name_pair = item.split(':')[1]
                self.name = name_pair.split('=')[1]
                continue
            key = item.split('=')[0]
            value = item.split('=')[1]
            if key == 'status':
                self.status = value
            elif key == 'address':
                self.address = value
            elif key == 'slaves':
                self.slaves = int(value)
            elif key == 'sentinels':
                self.sentinels = int(value)
        return self

    def __str__(self):
        return ('MasterInfo master_idx: {}, name: {}, status: {}, address: {}, slaves: {}, sentinels: {}'
                .format(self.master_idx, self.name, self.status, self.address, self.slaves, self.sentinels))


def get_target_shard(all_shard_line):
    """
    从 sentinel info 命令的回复中截取最多100个实例名称
    :param all_shard_line: sentinel info命令返回的行，格式是下面这样的：
        master828:name=SDIPPAYC_SIT_1,status=ok,address=10.237.161.219:6379,slaves=1,sentinels=3
    :return: 目标 shard 名称
    """
    target_shard_tmp = []
    for line in all_shard_line:
        if line.find('sentinel_masters') != -1:
            master_num = line.split(':')[1]
            print(' OK: The number of masters in observer sentinel before filtering is: {}'.format(master_num))
        if line.find('sentinel_tilt') != -1:
            tilt_mode = line.split(':')[1]
            if str(tilt_mode).strip() != TILT_MODE:
                print('ERR: observer sentinel is currently in tilt mode, exit script!!! {}'.format(line))
                sys.exit(0)
        if line.find('master') == -1:
            continue
        if line.find('status=ok') == -1:
            continue
        # master958:name=RCDCHS_1,status=ok,address=10.243.50.194:6379,slaves=1,sentinels=3
        master = MasterInfo().parse(line)
        # 找到正常的合法的实例
        target_shard_tmp.append(master)

    if len(target_shard_tmp) >= 50:
        # 实例数大于50，则每10个调一个
        target_shard_tmp = target_shard_tmp[0:len(target_shard_tmp):10]

    if len(target_shard_tmp) >= 100:
        # 实例数在10选1之后依旧大于100，则截断
        target_shard_tmp = target_shard_tmp[:100]
    print(' OK: The number of masters in observer sentinel after  filtering is: {}'.format(len(target_shard_tmp)))
    return target_shard_tmp


def execute_cmd(cmd):
    """
    执行指定的命令并返回执行结果
    :param cmd: 命令
    :return: 执行结果
    """
    try:
        ret_cmd = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf-8')
        return 0, 'cmd exec successfully', ret_cmd
    except subprocess.CalledProcessError as e:
        return e.returncode, str(e.output), None


class SentinelInstance:
    """
    存储 sentinel sentinels 命令返回的相关信息。
    """

    def __init__(self, ip=None, port=26379, flags=None, last_ok_ping_reply=None, last_hello_message=None):
        self.ip = ip
        self.port = port
        self.flags = flags
        self.last_ok_ping_reply = last_ok_ping_reply
        self.last_hello_message = last_hello_message

    def is_ok(self):
        """
        检查当前sentinel是否正常
        :return:
        """
        if self.flags != OK_FLAGS:
            return False, 'target sentinel flags is incorrect, correct: {}, current: {}'.format(OK_FLAGS, self.flags)

        if int(self.last_ok_ping_reply) > MAX_LAST_OK_PING_REPLY:
            return (False, 'last_ok_ping_reply too large, max: {}, current: {}'
                    .format(MAX_LAST_OK_PING_REPLY, self.last_ok_ping_reply))

        if int(self.last_hello_message) > MAX_LAST_HELLO_MESSAGE:
            return (False, 'last_hello_message too large, max: {}, current: {}'
                    .format(MAX_LAST_HELLO_MESSAGE, self.last_hello_message))

        return True, 'sentinel {}:{} is ok'.format(self.ip, self.port)

    def __str__(self):
        return ('SentinelInstance ip: {}, port: {}, flags: {}, last_ok_ping_reply: {}, last_hello_message: {}'
                .format(self.ip, self.port, self.flags, self.last_ok_ping_reply, self.last_hello_message))


def parse_sentinel_sentinel(lines_sentinel_sentinels, target_sentinel_ip):
    """
    解析 sentinel sentinels 命令的返回结果
    :param target_sentinel_ip: 待检查的目标  sentinel ip
    :param lines_sentinel_sentinels: sentinel sentinels命令返回的原始数据。原始返回如下：
            1)  1) "name"
            2) "36c91acb157a7cb11562859117145aa523ed04e5"
            3) "ip"
            4) "10.237.188.71"
            5) "port"
            6) "26379"
            7) "runid"
            8) "36c91acb157a7cb11562859117145aa523ed04e5"
            9) "flags"
           10) "sentinel"
           11) "link-pending-commands"
           12) "0"
           13) "link-refcount"
           14) "1021"
           15) "last-ping-sent"
           16) "0"
           17) "last-ok-ping-reply"
           18) "1025"
           19) "last-ping-reply"
           20) "1025"
           21) "down-after-milliseconds"
           22) "30000"
           23) "last-hello-message"
           24) "224"
           25) "voted-leader"
           26) "?"
           27) "voted-leader-epoch"
           28) "0"
           2) 1) "name"
              2) "61da927aabcc556e19610fba30345bdfcd50b7aa"
              ...
    :return: sentinelInstance 类型的实例
    """
    # 可能有空白字符串，清理一下
    lines_clean = []
    for line in lines_sentinel_sentinels:
        if line.strip():
            # 非空串，则添加到 lines_clean 中
            lines_clean.append(line)

    # sentinel 字典
    sentinel_dict_list = []
    # 字典序号
    sentinel_dict_no = -1
    idx = 0  # 遍历时的游标
    while idx < len(lines_clean):
        # 步长为2，因为字典中的信息都是key-value成对出现的
        key = lines_clean[idx]
        value = lines_clean[idx + 1]
        if key == 'name':
            # name是一个字典的开始，所以每碰到一个name就创建一个字典,并将字典序号+1
            sentinel_dict_no += 1
            sentinel_dict = {key: value}
            sentinel_dict_list.append(sentinel_dict)
        else:
            # 否则的话，从列表中取出字典，然后添加key-value
            sentinel_dict_list[sentinel_dict_no][key] = value
        idx += 2
    # 找到目标ip所在的字典
    target_sentinel_dict = None
    for sentinel_dict in sentinel_dict_list:
        if target_sentinel_ip == sentinel_dict.get('ip'):
            target_sentinel_dict = sentinel_dict
    if target_sentinel_dict is None:
        return False, 'can\'t find target sentinel from master\'s sentinel dict: {}'.format(target_sentinel_ip), None

    # 组装返回值
    sentinel_instance = SentinelInstance()
    sentinel_instance.ip = target_sentinel_dict.get('ip')
    sentinel_instance.port = target_sentinel_dict.get('port')
    sentinel_instance.flags = target_sentinel_dict.get('flags')
    sentinel_instance.last_ok_ping_reply = target_sentinel_dict.get('last-ok-ping-reply')
    sentinel_instance.last_hello_message = target_sentinel_dict.get('last-hello-message')
    return True, '', sentinel_instance


def cmd_args():
    """
    接收命令行参数
    :return:
    """
    parser = argparse.ArgumentParser(description="check sentinel")  # 使用argparse的构造函数来创建对象
    parser.add_argument("-o", "--observer", help="observer sentinel ip")  # 添加可解析的参数
    parser.add_argument("-t", "--target", help="target sentinel ip todo check", required=True)
    parser.add_argument("-m", "--mode", help="script run mode, normal or test, When the value is test, "
                                             "the script will use a more lenient way to check, such as do not ping the "
                                             "sentinel node")
    args = parser.parse_args()
    print(' OK: cmd args: {}'.format(args))

    # 获取参数中的运行模式
    global MODE
    if args.mode is not None:
        MODE = args.mode.strip().lower()
    print(' OK: script run mode is {}'.format(MODE))

    if args.observer is None:
        # 如果命令行没有指定观察者sentinel，则尝试远程获取
        args.observer = try_get_observer(args.target)

    # 校验参数中的ip地址是否合法
    if not ip_ok(args.observer):
        print('ERR: observer sentinel ip is illegal: {}'.format(args.observer))
        sys.exit(0)
    if not ip_ok(args.target):
        print('ERR: target sentinel ip is illegal: {}'.format(args.target))
        sys.exit(0)

    return args.observer, args.target


def try_get_observer(target_sentinel):
    """
    尝试远程获取观察者Sentinel，接口返回结果如下
            {
            "result": true,
            "msg": "",
            "data": [
                [
                    "GROUP_CONCAT(ip)"
                ],
                [
                    "10.237.188.57,10.237.188.58,10.237.188.6"
                ]
            ],
            "count": null,
            "baseId": null,
            "sentinelId": null,
            "sentinelCount": null
        }
    :param target_sentinel: 要检查的目标sentinel。
    :return: 观察者 sentinel ip
    """
    print(' OK: try get observer sentinel from remote...')
    # 从远端获取sentinel信息
    url = 'http://snrsadmin.cnsuning.com/snrs/sql/safe'
    headers = {'Content-Type': 'application/json'}
    post_data = {
        "secretKey": "KiEyYYMdIhseLsMXbAhCmFHMjLwQqXrXRSrQfhsmZiHCKluxwZpPYuMHtJQjTZpp",
        "data": SQL_T_QUERY_SENTINEL_CLUSTER.format(target_sentinel)
    }
    json_data = json.dumps(post_data, encoding="utf-8")
    req = urllib2.Request(url, json_data, headers)
    response = urllib2.urlopen(req, timeout=5)
    json_response = json.loads(response.read())
    # json_response['data'][1][0] ==> u'10.237.188.57,10.237.188.58,10.237.188.6'
    ip_list = json_response['data'][1][0]
    if ip_list is None:
        print('ERR: get observer sentinel cluster fail, sentinel cluster is None')
        sys.exit(0)
    ip_list = ip_list.split(',')  # 这个不区分类型可太牛逼了
    print(' OK: remote sentinel cluster: {}'.format(ip_list))

    # 尝试去ping一下所有的sentinel节点，至少能ping通，否则后面的检查没有意义
    if MODE == 'normal':
        ping_sentinels(ip_list)

    # 去除目标sentinel 节点
    if target_sentinel in ip_list:
        ip_list.remove(target_sentinel)
    if len(ip_list) == 0:
        print('ERR: observer sentinel cluster only have target sentinel: {}'.format(ip_list))
        sys.exit(0)

    # 获取剩下的第一个作为观察者 sentinel
    observer_sentinel = ip_list[0]
    if not ip_ok(observer_sentinel):
        print('ERR: observer sentinel ip from remote is illegal: {}'.format(observer_sentinel))
    print(' OK: observer sentinel from remote is {}'.format(observer_sentinel))
    return observer_sentinel


def ping_sentinels(sentinel_ips):
    """
    先ping一下获取到的sentinel列表，看一下其服务是否正常。
    :param sentinel_ips:
    :return:
    """
    for ip in sentinel_ips:
        print(' OK: try send a ping to sentinel: {} ...'.format(ip))
        CMD_T_PING[2] = ip
        code, msg, data = execute_cmd(CMD_T_PING)
        if code != 0:
            print('ERR: ping sentinel {} fail, exitCode: {}, stderr: {}'.format(ip, code, msg))
            print('ERR: Exit the script run!')
            sys.exit(0)
        if data.strip().lower() != 'pong':
            print('ERR: ping sentinel {} fail, exit script'.format(ip))
            sys.exit(0)
        print(' OK: ping {} successfully!'.format(ip))


def ip_ok(ip_addr):
    compile_ip = re.compile(
        '^(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|[1-9])\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)\.(1\d{2}|2[0-4]\d|25[0-5]|[1-9]\d|\d)$')
    if compile_ip.match(ip_addr):
        return True
    else:
        return False


def info_sentinels(observer_sentinel):
    """
    生成 具体的 info sentinel 命令
    :param observer_sentinel: 观察者sentinel
    :param shard_name: shard_name
    :return: CMD_T_INFO_SENTINEL = ['redis-cli', '-h', @observer_sentinel, '-p', '26379', 'info', 'sentinel']
    """
    CMD_T_INFO_SENTINEL[2] = observer_sentinel
    return CMD_T_INFO_SENTINEL


def sentinel_sentinels(observer_sentinel, shard_name):
    """
    生成 具体的 sentinel sentinels命令
    :param observer_sentinel: 观察者sentinel
    :param shard_name: shard_name
    :return: CMD_SENTINEL_SENTINELS = ['redis-cli', '-h', @observer_sentinel, '-p', '26379', 'sentinel', 'sentinels', @shard_name]
    """
    CMD_T_SENTINEL_SENTINELS[2] = observer_sentinel
    CMD_T_SENTINEL_SENTINELS[-1] = shard_name
    return CMD_T_SENTINEL_SENTINELS


def main():
    """
    主函数
    :return: None
    """
    # 从命令行获取观察者sentinel和目标sentinel
    observer, target = cmd_args()

    # 作为观察者的 sentinel
    observer_sentinel = observer
    # 待检查的目标 sentinel， 待检查的sentinel和观察者sentinel必须处于同一个集群中
    target_sentinel = target

    # 1、执行 info sentinel, 从sentinel中获取监控的shard列表
    cmd_info_sentinel = info_sentinels(observer_sentinel)
    code, msg, data = execute_cmd(cmd_info_sentinel)
    if code != 0:
        print('ERR: execute {} error, exitCode: {}, stderr: {}'.format(cmd_info_sentinel, code, msg))
        sys.exit(0)
    lines_info_sentinel = str(data).split('\n')

    # 2、解析info sentinel的返回结果，得到目标shard名称列表，最多的不超过100
    target_shard = get_target_shard(lines_info_sentinel)
    target_shard_count = len(target_shard)
    if target_shard_count == 0:
        print('ERR: sentinel {} can\'t find heath shard, len(target_shard) is: {}'
              .format(target_sentinel, target_shard_count))
        sys.exit(0)

    # 3、以当前集群中的观察者sentinel的视角检查目标sentinel是否正常
    for i, shard in enumerate(target_shard):
        shard_name = shard.name
        sentinel_num = shard.sentinels
        process = '[' + str(i + 1) + '/' + str(target_shard_count) + ']'
        # 3.1、检查 sentinel 数量
        if sentinel_num != SENTINEL_NUM_CORRECT:
            print('ERR: {}\t{}\tsentinel num is incorrect, correct: {}, current: {}'
                  .format(process.ljust(9), shard_name.ljust(16), SENTINEL_NUM_CORRECT, sentinel_num))
            continue

        # 3.2、检查 sentinel sentinels 返回中的相关指标
        cmd_sentinel_sentinels = sentinel_sentinels(observer_sentinel, shard_name)
        code, msg, data = execute_cmd(cmd_sentinel_sentinels)
        if code != 0:
            print('ERR: {}, exitCode: {}, stderr: {}'.format(shard_name, code, msg))
            continue
        if str(data).find('ERR') != -1:
            print('ERR: {}, sentinel return is incorrect: {}'.format(shard_name, data))
            continue

        lines_sentinel_sentinel = str(data).split('\n')
        ok, msg, sentinel_instance = parse_sentinel_sentinel(lines_sentinel_sentinel, target_sentinel)
        # 解析sentinel字典信息不正确，认为检测失败
        if not ok:
            print('ERR: {}, {}'.format(shard_name, msg))
            continue

        ok, msg = sentinel_instance.is_ok()
        if ok:
            print(' OK: {}\t{}'.format(process.ljust(9), shard_name.ljust(16)))
        else:
            print('ERR: {}\t{}\t{}'.format(process.ljust(9), shard_name.ljust(16), msg))


if __name__ == '__main__':
    main()
