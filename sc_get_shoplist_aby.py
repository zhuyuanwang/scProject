# coding=utf-8
import redis
import time
import requests
import os
import uuid
import traceback
import platform
import threading
from lxml import etree

# 连接Redis数据库
if "Win" in platform.system():
    RedisAddress = '127.0.0.1'
    RedisPort = 6379
    RedisDb = 6
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, db=RedisDb, encoding='utf-8')
else:
    RedisAddress = '10.*.*.*'
    RedisPort = 6379
    RedisDb = 7
    RedisPassword = '******'
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, password=RedisPassword, db=RedisDb, encoding='utf-8', decode_responses=True)

class mainSpider:
    """
    # 58同城
    # 示例 ：https://sz.58.com/ 深圳58同城网
    获取列表页
    """
    def __init__(self):
        self.thread_num = 2
        self.errornum = 0
        self.errorurlnum = 0
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36"
        }
        self.proxyHost = "http-dyn.abuyun.com"
        self.proxyPort = "9020"
        # 代理隧道验证信息
        self.proxyUser = ""
        self.proxyPass = ""

        self.proxyMeta = "http://%(user)s:%(pass)s@%(host)s:%(port)s" % {
            "host": self.proxyHost,
            "port": self.proxyPort,
            "user": self.proxyUser,
            "pass": self.proxyPass,
        }
        self.proxies = {
            "http": self.proxyMeta,
            "https": self.proxyMeta,
        }

    def get_shopurl(self, thread_num):
        while True:
            time.sleep(2)
            cityurl = redis_conn.spop("pageurl_set")
            PGTID = "PGTID={}&ClickID=1".format(str(uuid.uuid1()))
            if not cityurl:
                self.errorurlnum += 1
                time.sleep(10)
                print('errornum:', self.errorurlnum)
                if self.errorurlnum > 5:
                    print('任务结束')
                    print('目前的errorurlnum：', self.errorurlnum)
                    os._exit(1)
                    break
                continue
            else:
                self.errornum = 0

                time.sleep(1)
                print('当前url：', cityurl)
                print('当前线程数：', thread_num)
                # proxies = {"https": "https://" + str(self.IP_list[str(thread_num)])}
                try:
                    # print('当前使用的代理IP:',self.proxies)
                    self.headers['authority'] = cityurl.split('/')[0].replace('https://', '')
                    self.headers['referer'] = cityurl.split('/pn')[0]
                    res = requests.get(url=cityurl, headers=self.headers, proxies=self.proxies, timeout=3)
                    city = cityurl.split('//')[-1].split('.')[0]
                    if "没有找到相关的房源" in res.text:
                        redis_conn.lpush('noshopurl', cityurl)
                        print('当前url没有找到房源')
                        continue
                    if "访问过于频繁" in res.text:
                        print('当前代理IP失效')
                        redis_conn.sadd('pageurl_set', cityurl)
                        continue

                    res_html = etree.HTML(res.text)
                    # 存在临时链接，所以采用第二种方法获取详情链接
                    details_url_1 = res_html.xpath('//div[@class="content-side-left"]/ul/li//h2/a/@href')
                    details_url_2 = res_html.xpath('//div[@class="content-side-left"]/ul/li/@logr')
                    time.sleep(1)
                    for detail_urlcode in details_url_2:
                        detail_urldata = detail_urlcode.split('_sortid')[0].split('_')
                        if len(detail_urldata) == 4:
                            shopcode = detail_urldata[-1]
                        else:
                            shopcode = detail_urldata[-3]

                        detail_url = 'https://{}.58.com/shangpu/{}x.shtml'.format(city, str(shopcode))
                        # print(detail_url)
                        if redis_conn.sadd('all_detailsurl_set', detail_url):
                            redis_conn.sadd('detailsurl_set', detail_url)
                    print('--分割线--')
                    for detail_url1 in details_url_1:
                        detail_url1 = detail_url1.split('?houseId')[0]
                        # print(detail_url1)
                        if redis_conn.sadd('all_detailsurl_set', detail_url1):
                            redis_conn.sadd('detailsurl_set', detail_url1)
                    print('存入成功')

                except:
                    print('---**---')
                    traceback.print_exc()
                    print('---**---')
                    time.sleep(3)
                    self.errornum += 1
                    # print('当前线程数：',thread_num)
                    # print('当前代理：',self.proxies)
                    redis_conn.sadd('pageurl_set', cityurl)
                    print('error')
                    time.sleep(3)
                    if self.errornum > 10:
                        print('结束时间：', time.asctime())
                        print('当前errornum:', self.errornum)
                        os._exit(1)
                        break
                    continue


    def checkthread(self, initThreadsName):
        # 检查线程状态
        while True:

            newThreadsName = []
            for i in threading.enumerate():
                # TODO 记录正在运行的线程
                newThreadsName.append(i.getName())

            # TODO 判断有没有线程中途挂掉 如果有就重启线程
            for oldname in initThreadsName:
                if oldname in newThreadsName:
                    pass
                else:
                    print('older:',oldname)
                    print('oldname的type：',type(oldname))
                    thread = threading.Thread(target=self.get_shopurl, args=oldname)
                    thread.setName(oldname)
                    thread.start()
                    print('重新启动了线程:{}'.format(oldname))
            time.sleep(3)

    def thread_start(self):
        # self.whether_task()
        global recursion_num
        thread_list = []
        init_thread_name = []  # TODO 记录线程名
        for i in range(self.thread_num):
            thread = threading.Thread(target=self.get_shopurl, args=str(i))
            # TODO 给线程赋值
            # thread.setName(str(i))
            thread.setName(str(i))
            thread_list.append(thread)

        # 启动线程
        for thread in thread_list:
            thread.start()
            time.sleep(0.5)

        # TODO 获取初始化的线程对象
        init = threading.enumerate()
        for i in init:
            # TODO 保存初始化线程名字
            init_thread_name.append(i.getName())
        thread_pro = threading.Thread(target=self.checkthread, args=(init_thread_name,))
        thread_pro.start()

        for thread in thread_list:
            thread.join()

        self.thread_start()
        thread_pro.join()


if __name__ == '__main__':
    s = mainSpider()
    s.thread_start()