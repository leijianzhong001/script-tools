# 这是一个示例 Python 脚本。

import pandas as pd
import numpy as np

# 创建一个 DataFrame
master1158 = {'name': 'RRP_SIT_1', 'status': 'odown', 'address': '10.237.170.61:6379', 'slaves': 0, 'sentinels': 3}
master1170 = {'name': 'PS_master_1', 'status': 'sdown', 'address': '10.37.64.166:6379', 'slaves': 0, 'sentinels': 1}
master1178 = {'name': 'OSMOS_SIT_2', 'status': 'odown', 'address': '10.237.36.125:6379', 'slaves': 0, 'sentinels': 3}
dict_data = [master1158, master1170, master1178]

data = pd.DataFrame(dict_data, index=['name', 'status', 'address', 'slaves'])
print(data)
