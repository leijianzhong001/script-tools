# 这是一个示例 Python 脚本。

import requests
import json

url = "http://snrsadmin.cnsuning.com/snrs/sql/safe"

payload = json.dumps({
  "secretKey": "KiEyYYMdIhseLsMXbAhCmFHMjLwQqXrXRSrQfhsmZiHCKluxwZpPYuMHtJQjTZpp",
  "data": "select * from  snrs_config_items where config_type='SnrsMetaDataChecker' and config_name_en='switch';"
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)
