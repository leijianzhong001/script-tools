# -*- coding:utf-8 -*-

import redis

standalone_host = '10.243.66.12'
standalone_port = 6380
redisClientStandalone = redis.Redis(host=standalone_host, port=standalone_port, decode_responses=True)
isOk = redisClientStandalone.ping()
if isOk:
    print(f'Redis Server {standalone_host}:{standalone_port} is ok')

cluster_host = '10.243.66.12'
cluster_port = 30001
redisClientCluster = redis.cluster.RedisCluster(host=cluster_host, port=cluster_port, decode_responses=True)
cluster_info = redisClientCluster.cluster_info()

if cluster_info.get('cluster_state') == 'ok':
    print(f'Redis Cluster Server {cluster_host}:{cluster_port} is ok')
