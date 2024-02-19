# -*- coding:utf-8 -*-
from fabric import Connection

# 远程主机的连接信息
host = '10.243.66.12'
username = 'root'
password = 'rsrzrcj430223!'

# 创建远程连接
conn = Connection(host=host, user=username, connect_kwargs={'password': password})

# 从远程机器上拷贝JDK安装包到当前机器的/tmp目录
conn.get('/path/to/jdk.tar.gz', '/tmp/jdk.tar.gz')

# 解压安装包到/usr/local/目录
conn.run('tar -xf /tmp/jdk.tar.gz -C /usr/local/')

# 修改安装目录及其子目录的用户和用户组为dtsfsys
conn.run('chown -R dtsfsys:dtsfsys /usr/local/jdk')

# 配置JDK环境变量到/etc/profile中
conn.run('echo "export JAVA_HOME=/usr/local/jdk" >> /etc/profile')
conn.run('echo "export PATH=$PATH:$JAVA_HOME/bin" >> /etc/profile')

# 生效环境变量
conn.run('source /etc/profile')
