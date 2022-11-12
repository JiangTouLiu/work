#测试导出全部数据
import requests
import json
import time
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter 
import pymysql

#获取当前时间，用来命名日志文件
current_time = time.localtime()
#根据获取到的时间格式出文件名
log_file = f'{current_time.tm_mon}-{current_time.tm_mday}-{current_time.tm_hour}-{current_time.tm_min}'
#定义日志格式，输入文件
logging.basicConfig(filename=f'{log_file}.txt', filemode='w', format='%(asctime)s-%(name)s -%(levelname)s :%(message)s', level=logging.INFO)
#定义requests请求的重试信息
retries = Retry(
    total = 10,
    status_forcelist=[400,401,403,429,500,503]
)
     
s = requests.session()
s.mount('http://', HTTPAdapter(max_retries=retries))
s.mount('https://', HTTPAdapter(max_retries=retries))

#定义获取token的函数
def get_token(username, password):
     token_data = {
        "username": f"{username}",
        "password": f"{password}",
        "grant_type": "password"
    }
     token_headers = {
     "Content-Type":"application/json"
     }
     
     token_response = s.post('https://openapi.bazhuayu.com/token', headers=token_headers, json=token_data)
     if token_response.status_code != 200:
          logging.info('get token failed status_code %d while get token', token_response.status_code)
     else:
          logging.info('get token successed status_code %d while get token', token_response.status_code)
          
     token_json = token_response.json()
     return token_json['data']

#定义刷新token的函数
def refresh_token(refresh):
     refresh_data = {
          "refresh_token": f"{refresh}",
          "grant_type": "refresh_token"
       }
     token_headers = {
     "Content-Type":"application/json"
     }
     token_response = s.post('https://openapi.bazhuayu.com/token', headers=token_headers, json=refresh_data)
     if token_response.status_code != 200:
          logging.info('refresh token failed status_code %d while get token', token_response.status_code)
     else:
         logging.info('refresh token successed status_code %d while get token', token_response.status_code)
          
     token_json = token_response.json()
     return token_json['data']
