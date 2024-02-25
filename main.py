# 这是一个示例 Python 脚本。
import os
import re
import sys
from bs4 import BeautifulSoup
import requests
from concurrent.futures import ThreadPoolExecutor
import openpyxl
from openpyxl.workbook import Workbook
import pymysql.cursors

db_list = [{}, {}]
sys_en_list = [db.get('sys_en') for db in db_list]
print(sys_en_list)
print(','.join(f'{a}' for a in sys_en_list))
