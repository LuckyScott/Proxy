#!/usr/bin/env python
# -*- coding:utf-8 -*-

# proxy_fetch.py

import requests
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
import time
import json
import base64
try:
    from json import JSONDecodeError
except:
    from simplejson import JSONDecodeError


DB_CONNECT_CFG = {
    "db_engine" : "postgresql",
    "host" : "192.168.1.10",
    "port" : 5432,
    "username" : "postgres",
    "password" : "postgres",
    "database" : "fdc_development"
}

SPIDER_ID = '88888888888888888888888888888888'
ORDER_NO = '000000000000000'

WANDOU_USERNAME = 'username'
WANDOU_PASSWORD = 'password'
WANDOU_APP_KEY = None

FETCH_COUNT = 20 # 单次提取数量
FETCH_INTERVAL = 5 # 提取间隔时间，单位：分钟
CHECK_URL = 'http://myip.ipip.net/'
USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36'

class DBM(object):
    def __init__(self, **connect_config):
        self.engine = None
        # self.session = None
        self.connect(**connect_config)

    def connect(self, db_engine=None, host='127.0.0.1', port=5432, username='postgres', password='', database='postgres'):
        if db_engine is None:
            raise TypeError('Argument db_engine is None!')
        conn_url = '{db_engine}://{username}:{password}@{host}:{port}/{database}'.format(db_engine=db_engine, host=host, port=port, username=username, password=password, database=database)
        self.engine = create_engine(conn_url, encoding='utf-8', max_overflow=5)
        # db_session = sessionmaker(bind=self.engine)
        # self.session = db_session()

    def close(self):
        # self.session.close()
        # conn = self.engine.connect()
        # conn.close()
        self.engine.dispose()

    def execute_sql(self, sql_text, args=None):
        # self.session.execute(text(sql_text), args)
        # self.session.commit()
        conn = self.engine.connect()
        conn.execute(text(sql_text))
        conn.close()


class ProxyPool(object):
    def __init__(self, dbconnect=None, **kwargs):
        self.db = DBM(**dbconnect)
        self.check_url = kwargs.get('check_url', 'http://myip.ipip.net')
        self.user_agent = kwargs.get('user_agent', 'Mozilla/5.0 (Windows NT 6.3; Trident/7.0; rv:11.0) like Gecko')

    def clean_invalid_ips(self):
        sql = 'DELETE FROM valid_ips where can_ths = FALSE;'
        self.db.execute_sql(sql)

    def check_proxy_ip(self, proxy_ip, proxy_port, check_url=None, result_check=None, proxy_auth=None):
        '''
        proxy_auth: username:password
        '''
        try:
            headers = {
                "User-Agent": self.user_agent
            }
            if proxy_auth is not None:
                username, password = proxy_auth.split(':')
                headers['Proxy-Authorization'] = 'Basic %s' % base_code(username, password)

            proxies = {
                'http': 'http://%s:%s' % (proxy_ip, proxy_port),
                'https': 'https://%s:%s' % (proxy_ip, proxy_port)
            }
            url = self.check_url if check_url is None else check_url
            res = requests.get(url, proxies=proxies, headers=headers, timeout=5)
            if res.ok and res.text != '':
                if callable(result_check):
                    return result_check(res)
            return False
        except Exception as e:
            print("Check Proxy Error: {}".format(e))
            return False

    # 讯代理
    def get_xdaili_iplist(self, spider_id, orderno, return_type=2, count=20):
        url = 'http://api.xdaili.cn/xdaili-api//greatRecharge/getGreatIp?'
        params = {
            'spiderId': spider_id,
            'orderno': orderno,
            'returnType': return_type,
            'count': count
        }

        try:
            res = requests.get(url, params=params)
            res_json = res.json()
            if int(res_json['ERRORCODE']) != 0:
                print("Xdaili Fetch proxies Error: {}".format(res.text))
                return []
            proxy_list = res_json['RESULT']
        except JSONDecodeError as e:
            if return_type == 1: # return txt
                proxy_list = []
                for v in res.text.strip().split('\r\n'):
                    ip, port = v.strip().split(':')
                    proxy_list.append({'ip': ip, 'port': port})
            else:
                print("Xdaili Fetch proxies Json Parse Error: {}".format(e))
                return []
        except Exception as e:
            print("Xdaili Fetch proxies Error: {}:{}".format(type(e), e))
            return []

        # check valid
        valid_proxies = []
        print("Fetched proxies count: {}".format(len(proxy_list)))
        for proxy in proxy_list:
            if self.check_proxy_ip(proxy['ip'], proxy['port'], result_check=can_ths_check):
                print(">>{}:{} check success".format(proxy['ip'], proxy['port']))
                valid_proxies.append(proxy)
            else:
                print(">>{}:{} check failed".format(proxy['ip'], proxy['port']))
        print("=====Finally Get valid proxies count: {}=====".format(len(valid_proxies)))
        return valid_proxies

    def save_proxy_iplist(self, proxies):
        count = 0
        total_count = 0
        for proxy in proxies:
            if count == 0:
                sql = "insert into valid_ips (content, test_times, failure_times, success_rate, avg_response_time, score, created_at, updated_at, true_ip, can_ths) values "
                sql += "('{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(ip=proxy['ip'], port=proxy['port'])
            else:
                sql += ",('{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(ip=proxy['ip'], port=proxy['port'])
            count += 1
            if count >= 10:
                sql += ';'
                self.db.execute_sql(sql)
                count = 0
            total_count += 1
        if count > 0:
            sql += ';'
            self.db.execute_sql(sql)

    # 豌豆代理
    def get_wandouip_list(self, app_key=None, num=20, return_type='json', xy=1, port=None, mr=2):
        url = 'https://h.wandouip.com/get/ip-list?'
        return_type = 2 if return_type == 'json' else 1
        params = {
            'pack': 0,
            'num': num,
            'type': return_type,
            'xy': xy,
            'lb': '\n',
            'mr': mr
        }

        if isinstance(app_key, str):
            params['app_key'] = app_key
        if isinstance(port, int):
            params['port'] = port

        try:
            res = requests.get(url, params=params)
            res_json = res.json()
            if int(res_json['code']) != 200:
                print("Wandouip Fetch proxies Error: {}".format(res.text))
                return []
            proxy_list = res_json['data']
        except JSONDecodeError as e:
            if return_type == 1: # return txt
                proxy_list = []
                for v in res.text.strip().split('\n'):
                    ip, port = v.strip().split(':')
                    proxy_list.append({'ip': ip, 'port': port, 'expire_time': None, 'city': None, 'isp': None})
            else:
                print("Wandouip Fetch proxies Json Parse Error: {}".format(e))
                return []
        except Exception as e:
            print("Wandouip Fetch proxies Error: {}:{}".format(type(e), e))
            return []

        # check valid
        valid_proxies = []
        print("Fetched proxies count: {}".format(len(proxy_list)))
        proxy_auth = '%s:%s' % (WANDOU_USERNAME, WANDOU_PASSWORD)
        for proxy in proxy_list:
            if self.check_proxy_ip(proxy['ip'], proxy['port'], result_check=can_ths_check, proxy_auth=proxy_auth):
                print(">>{}:{} check success".format(proxy['ip'], proxy['port']))
                valid_proxies.append(proxy)
            else:
                print(">>{}:{} check failed".format(proxy['ip'], proxy['port']))
        print("=====Finally Get valid proxies count: {}=====".format(len(valid_proxies)))
        return valid_proxies

    def save_wandouip_proxy_list(self, proxies):
        count = 0
        total_count = 0
        for proxy in proxies:
            if count == 0:
                sql = "insert into valid_ips (username, password, expire_time, city, isp, content, test_times, failure_times, success_rate, avg_response_time, score, created_at, updated_at, true_ip, can_ths) values "
                if proxy['expire_time'] is None:
                    sql += "('{username}', '{password}', NULL, NULL, NULL,'{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(username=WANDOU_USERNAME, password=WANDOU_PASSWORD, ip=proxy['ip'], port=proxy['port'])
                else:
                    sql += "('{username}', '{password}', '{expire}','{city}','{isp}','{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(username=WANDOU_USERNAME, password=WANDOU_PASSWORD, expire=proxy['expire_time'],city=proxy['city'],isp=proxy['isp'],ip=proxy['ip'], port=proxy['port'])
            else:
                if proxy['expire_time'] is None:
                    sql += ",('{username}', '{password}', NULL, NULL, NULL,'{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(username=WANDOU_USERNAME, password=WANDOU_PASSWORD, ip=proxy['ip'], port=proxy['port'])
                else:
                    sql += ",('{username}', '{password}', '{expire}','{city}','{isp}','{ip}:{port}', 1, 0, 1, 2.5, 0, now(), now(), '{ip}', true)".format(username=WANDOU_USERNAME, password=WANDOU_PASSWORD, expire=proxy['expire_time'],city=proxy['city'],isp=proxy['isp'],ip=proxy['ip'], port=proxy['port'])
            count += 1
            if count >= 10:
                sql += ';'
                self.db.execute_sql(sql)
                count = 0
            total_count += 1
        if count > 0:
            sql += ';'
            self.db.execute_sql(sql)

    def fetch_xdaili_iplist(self, spider_id, orderno, return_type=2, count=20):
        # self.clean_invalid_ips()
        ip_list = self.get_xdaili_iplist(spider_id, orderno, return_type=return_type, count=count)
        self.save_proxy_iplist(ip_list)
        self.db.close()

    def fetch_wandouip(self, app_key=None, num=20, return_type='json'):
        # self.clean_invalid_ips()
        ip_list = self.get_wandouip_list(app_key=app_key, num=num, return_type=return_type)
        self.save_wandouip_proxy_list(ip_list)
        self.db.close()


def can_ths_check(response):
    try:
        res_json = response.json()
        return res_json.get('errorCode') == 0
    except Exception as e:
        print("Error: {}".format(e))
        return False

def base_code(username, password):
    rawstr = '%s:%s' % (username, password)
    encodestr = base64.b64encode(rawstr.encode('utf-8'))
    return '%s' % encodestr.decode()


def run():
    while True:
        proxy_pool = ProxyPool(DB_CONNECT_CFG, check_url=CHECK_URL, user_agent=USER_AGENT)
        # proxy_pool.fetch_xdaili_iplist(SPIDER_ID, ORDER_NO, count=FETCH_COUNT)
        proxy_pool.fetch_wandouip(app_key=WANDOU_APP_KEY, num=FETCH_COUNT)
        time.sleep(FETCH_INTERVAL * 60)


if __name__ == "__main__":
    # run()
    proxy_pool = ProxyPool(DB_CONNECT_CFG, check_url=CHECK_URL, user_agent=USER_AGENT)
    # ip_list = proxy_pool.get_wandouip_list(app_key=WANDOU_APP_KEY, num=10)
    # print(ip_list)
    proxy_pool.fetch_wandouip(app_key=WANDOU_APP_KEY, num=10)

