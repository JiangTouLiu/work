import requests
import json
import time
import logging
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter 
import pymysql
from pymysql.converters import escape_string







current_time = time.localtime()
log_file = f'{current_time.tm_mon}-{current_time.tm_mday}-{current_time.tm_hour}-{current_time.tm_min}'
logging.basicConfig(filename=f'{log_file}.txt', filemode='w', encoding='utf-8', format='%(asctime)s-%(name)s -%(levelname)s :%(message)s', level=logging.INFO)

class BzyUser:
    userName = ''  # 八爪鱼用户名
    passwd = ''     # 八爪鱼密码
    token_all = ''
    token = token_all
    retries = Retry(
        total = 10,
        status_forcelist=[400,401,403,429,500,503]
    )
    s = requests.session()
    s.mount('http://', HTTPAdapter(max_retries=retries))
    s.mount('https://', HTTPAdapter(max_retries=retries))

    def __init__(self, userName, passwd) -> None:
        self.userName = userName
        self.passwd = passwd 
        self.token_all = self.get_token()
        self.token = self.token_all['access_token']

    def get_token(self):
        token_data = {
            "username": f"{self.userName}",
            "password": f"{self.passwd}",
            "grant_type": "password"
        }
        token_headers = {
        "Content-Type":"application/json"
        }
        token_response = self.s.post('https://openapi.bazhuayu.com/token', headers=token_headers, json=token_data)
        if token_response.status_code != 200:
            logging.info('get token failed status_code %d while get token', token_response.status_code)
        else:
            logging.info('get token successed status_code %d while get token', token_response.status_code)
            
        token_json = token_response.json()
        return token_json['data']   
    
    def get_non_data(self, taskid, size):
        headers = {
        "Authorization": f"Bearer {self.token}"
        }
        params = {'taskid':f'{taskid}', 'size':f'{size}'}
        
        data_response = self.s.get('https://openapi.bazhuayu.com/data/notexported', headers=headers, params=params)
        while data_response.status_code == 401:
            self.token_all = self.get_token()
            self.token = self.token_all['access_token']
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }   
            data_response = self.s.get('https://openapi.bazhuayu.com/data/notexported', headers=headers, params=params)
        if data_response.status_code != 200:
            logging.info('get data failed status_code %d while get data', data_response.status_code)
        else:
            logging.info('get data successed status_code %d while get data', data_response.status_code)
        orgin_data = data_response.json()
        return orgin_data['data']

    def mark_data(self, task_id):
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }
        data = {
            "taskId": f"{task_id}"
        }
        data_response = self.s.post('https://openapi.bazhuayu.com/data/markexported', headers=headers, json=data)
        while data_response.status_code == 401:
            self.token_all = self.get_token()
            self.token = self.token_all['access_token']
            headers = {
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json"
            }   
            data_response = self.s.post('https://openapi.bazhuayu.com/data/markexported', headers=headers, json=data)
        if data_response.status_code != 200:
            logging.info('markexported failed status_code %d while mark_data', data_response.status_code)
        else:
            logging.info('markexported successed status_code %d while mark_data', data_response.status_code)
        orgin_data = data_response.json()
        return orgin_data


if __name__ == "__main__":
    host = '127.0.0.1'
    user = 'username'
    passwd = 'password'
    database = 'auto_import_db' 
    charset = 'utf8mb4'
    bazhuayu = BzyUser('bzyuser', 'bzypasswd')
    task_id = 'taskid'
    data =  bazhuayu.get_non_data(task_id, 1)
    logging.info('total amount: %s', data['total'])

    j = 0
    db = pymysql.connect(host=host, user=user, passwd=passwd, database=database, charset=charset)
    cursor = db.cursor()
    while (data['total'] > 0):
    # 请求数据
        logging.info('请求前总数据量：%s', data['total'])
        data =  bazhuayu.get_non_data(task_id, 1)
        logging.info('请求后总数据量：%s', data['total'])
    # 导出到数据库
        for i in data['data']:
            insert_sql = "insert ignore into commodity (categoryid, site, title, price, color, size, stock, detail, product_url, pro_id, pro_img) values('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s')"%(escape_string(i["category_id"]), escape_string(i["站点"]), escape_string(i["商品标题"]), escape_string(i['价格']), escape_string(i['颜色']), escape_string(i['大小']), escape_string(i['库存']), escape_string(i['详情']), escape_string(i['页面网址']), escape_string(i["product_id"]), escape_string(i['图片链接']))
            cursor.execute(insert_sql)
            logging.info('1 data successfully import data:%s', i)
            db.commit()
            j += 1
        print('10 datas is import successfully')
        #标记数据
        bazhuayu.mark_data(task_id)
    cursor.close()
    db.close()
    print(f'{j}条数据成功导出到数据库')
    logging.info('%s data successfully import data:', j)

    
