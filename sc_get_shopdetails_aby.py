# coding=utf-8
import redis
import time
import requests
import datetime
import re
import os
import psycopg2
import platform
import threading
from lxml import etree

# 连接Redis数据库
if "Win" in platform.system():
    RedisAddress = '127.0.0.1'
    RedisPort = 6379
    RedisDb = 6
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, db=RedisDb, encoding='utf-8')
    conn = psycopg2.connect(database="postgres", host='127.0.0.1', user='postgres', password='******', port='5432', decode_responses=True)
else:
    RedisAddress = '10.*.*.*'
    RedisPort = 6379
    RedisDb = 7
    RedisPassword = '***'
    redis_conn = redis.Redis(host=RedisAddress, port=RedisPort, password=RedisPassword, db=RedisDb, encoding='utf-8', decode_responses=True)
    conn = psycopg2.connect(database="crawler", host='10.*.*.*', user='root', password='*****', port='8635')

class mainSpider:
    """
    # 58同城
    # 示例 ：https://sz.58.com/ 深圳58同城网
    详情页
    """
    def __init__(self):
        self.thread_num = 4
        self.errornum = 0
        self.errorurlnum = 0
        self.crawlnum = 0
        self.headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "upgrade-insecure-requests": "1",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.97 Safari/537.36",
        }

        self.proxyHost = "http-dyn.abuyun.com"
        self.proxyPort = "9020"
        # 代理隧道验证信息
        self.proxyUser = ""
        self.proxyPass = ""

        self.proxyMeta = "https://%(user)s:%(pass)s@%(host)s:%(port)s" % {
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
            time.sleep(1)
            cityurl = redis_conn.spop("detailsurl_set")
            if not cityurl:
                self.errorurlnum += 1
                print('errornum:', self.errorurlnum)
                if self.errorurlnum > 5:
                    time.sleep(1)
                    print('任务结束')
                    print('结束时间：', time.asctime())
                    os._exit(1)
                    break
                continue
            else:
                self.errornum = 0
                if cityurl.split('shangpu')[-1] == '1x.shtml':
                    break

            print('当前url：',cityurl)
            print('当前线程数：',thread_num)
            try:
                res = requests.get(url=cityurl, headers=self.headers,proxies=self.proxies, timeout=5)
                if "访问过于频繁" in res.text:
                    print('当前ip失效......')
                    redis_conn.sadd('detailsurl_set', cityurl)
                    continue
                elif "没有找到相关信息" in res.text:
                    print('当前url暂无详情信息')
                    redis_conn.sadd('nodetailsurl_set', cityurl)
                    continue

                else:
                    datadict = {}
                    datadict['url'] = cityurl
                    res_html = etree.HTML(res.text)
                    time.sleep(1)
                    # 标题
                    title = res_html.xpath('//div[@class="house-title"]/h1/text()')
                    datadict['title'] = title[0].replace('\xa0','') if title else ''
                    # TODO
                    # 面积
                    square = res_html.xpath('//p[@class="house_basic_title_info"]/span[1]/text()')
                    datadict['square'] = square[0] if square else ''
                    # 联系人
                    contract_name = res_html.xpath('//div[@class="poster-name"]/span/text()')
                    datadict['contract_name'] = contract_name[0].strip() if contract_name else ''
                    # 联系方式
                    contract_phone = res_html.xpath('//div[@class="house-chat-phone"]/p/text()')
                    datadict['contract_phone'] = contract_phone[0] if contract_phone else ''
                    # 联系方（公司或者个人）
                    contract_company = res_html.xpath('//p[contains(@class,"poster")]/text()')
                    datadict['contract_company'] = contract_company[0] if contract_company else ''
                    # 租金数额
                    rentnum = res_html.xpath('//div[@class="house-basic-info clearfix"]/div[@class="house-basic-right fr"]/p[@class="house_basic_title_money"]/span[@class="house_basic_title_money_num"]/text()')
                    rentnum = rentnum[0] if rentnum else ''
                    # 单位
                    rentunit = res_html.xpath('//div[@class="house-basic-info clearfix"]/div[@class="house-basic-right fr"]/p[@class="house_basic_title_money"]/span[@class="house_basic_title_money_unit"]/text()')
                    rentunit = rentunit[0] if rentunit else ''
                    datadict['rent'] = str(rentnum) + rentunit
                    # 标签
                    tags = res_html.xpath('//p[@class="house-update-info"]/span[not(contains(@class,"up"))]//text()')
                    datadict['tags'] = str(tags).replace("'",'') if tags else ''
                    # 更新时间
                    update_date = res_html.xpath('//p[@class="house-update-info"]/span[@class="up"][1]/text()')
                    datadict['update_date'] = update_date[0] if update_date else ''
                    # 数据来源
                    data_source = '58同城'
                    datadict['data_source'] = data_source
                    # 图片url
                    picture = res_html.xpath('//div[@class="house-basic-info clearfix"]//img[@id="smainPic"]/@src')
                    datadict['picture'] = picture[0] if picture else ''
                    # 店铺简介
                    description = res_html.xpath('//div[@class="general-item general-miaoshu"]/div//text()')
                    datadict['description'] = ''.join(description).replace('\n', '').replace('\t', '').replace('\r', '').replace('\xa0', '').replace(' ', '').strip() if description else ''
                    # 地址
                    address = res_html.xpath('//div[@class="house-basic-right fr"]/div[@class="house_basic_title_info_2"]/p[2]/span[2]/text()')
                    datadict['address'] = ''.join(address).strip() if address else ''
                    # TODO
                    # 片区
                    region = res_html.xpath('//div[@class="nav-top-bar fl c_888 f12"]/a[5]/text()')
                    datadict['region'] = region[0].split('商铺')[0] if region else ''
                    # TODO
                    # 行政区
                    district = res_html.xpath('//div[@class="nav-top-bar fl c_888 f12"]/a[4]/text()')
                    datadict['district'] = district[0].split('商铺')[0] if district else ''
                    # 区域
                    try:
                        area = district[0].split('商铺')[0] + '-' + region[0].split('商铺')[0]
                        datadict['area'] = area
                    except:
                        datadict['area'] = ''
                    # 城市
                    city = res_html.xpath('//div[@class="nav-top-bar fl c_888 f12"]/a[1]/text()')
                    datadict['city'] = city[0].replace('58同城','') if city else ''
                    # 经纬度
                    lat = re.findall('lat: (.*?),', res.text)
                    datadict['lat'] = lat[0] if lat else ''
                    lng = re.findall('lng: (.*?),', res.text)
                    datadict['lng'] = lng[0] if lng else ''
                    # 采集时间字段
                    datadict['collection_date'] = str(str(datetime.datetime.now()))
                    # 概括里面的数据
                    target = res_html.xpath('//div[@class="general-item general-intro"]/ul/li')
                    for i in range(1, len(target)):
                        targetxpath = '//div[@class="general-item general-intro"]/ul/li[{}]/span[1]/text()'.format(i)
                        targetcontent = res_html.xpath(targetxpath)[0]
                        if targetcontent == '商铺类型':
                            shop_type = res_html.xpath('//div[@class="general-item general-intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['shop_type'] = shop_type[0] if shop_type else ''
                        elif targetcontent == '商铺状态':
                            status = res_html.xpath('//div[@class="general-item general-intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['status'] = status[0] if status else ''
                        elif targetcontent == '押付':
                            mortgage_pay = res_html.xpath('//div[@class="general-item general-intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['mortgage_pay'] = mortgage_pay[0] if mortgage_pay else ''
                        elif targetcontent == '规格':
                            specs = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            specs = specs[0] if specs else ''
                            # 面宽
                            width = re.findall('面宽(.*?m)', specs)
                            datadict['width'] = width[0] if width else ''
                            # 层高
                            high = re.findall('层高(.*?m)', specs)
                            datadict['high'] = high[0] if high else ''
                            # 进深
                            length = re.findall('进深(.*?m)', specs)
                            datadict['length'] = length[0] if length else ''
                        elif targetcontent == '转让费':
                            transfer_fee = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['transfer_fee'] = transfer_fee[0].strip() if transfer_fee else ''
                        elif targetcontent == '经营状态':
                            run_status = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['run_status'] = run_status[0] if run_status else ''
                        elif targetcontent == '经营类型':
                            run_type = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['run_type'] = run_type[0] if run_type else ''
                        elif targetcontent == '楼层':
                            floor = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['floor'] = floor[0] if floor else ''
                        elif targetcontent == '商铺性质':
                            shop_nature = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['shop_nature'] = shop_nature[0] if shop_nature else ''
                        elif targetcontent == '起租期':
                            least_rent_period = res_html.xpath('//div[@class="house-detail-info"]/div[@class="house-detail-left"]/div[@id="intro"]/ul/li[{}]/span[2]/text()'.format(i))
                            datadict['least_rent_period'] = least_rent_period[0] if least_rent_period else ''
                    # print(datadict)
                    self.insert_data(datadict)
                    self.errornum = 0

            except:
                # traceback.print_exc()
                print('抓取失败.....重试中.....')
                # self.IP_list[str(thread_num)] = self.proxy_ip
                redis_conn.sadd('detailsurl_set', cityurl)
                time.sleep(3)
                self.errornum += 1
                # 记录爬取失败的url
                # redis_conn.lpush('errorscdetailsurlList', cityurl)
                print('error')
                if self.errornum > 10:
                    os._exit(1)
                    break
                continue

    def insert_data(self, datadict):
        self.crawlnum += 1
        valuesdict = [i for i in datadict]
        keysdict = [datadict[i] for i in datadict]
        valuesdictdata = (','.join(valuesdict))
        # 存储数据
        sql = 'insert into scrapy_tongcheng ({}) values ({})'.format(valuesdictdata, str(keysdict)[1:-1])
        try:
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            print('存储成功，第{}条数据'.format(str(self.crawlnum)))
            time.sleep(1)
        except Exception as e:
            redis_conn.sadd('detailsurl_set_last', datadict['url'])
            print('error_info:{}'.format(e))
            conn.rollback()

    def whether_task(self):
        # 判断任务列表是否为空
        if redis_conn.llen("scdetailsurlList") == 0:
            print('whether_task城市列表为空')
            os._exit(0)
        else:
            pass

    def checkthread(self, initThreadsName):
        # 检查线程状态
        while True:
            # self.whether_task()
            # if redis_conn.llen("scdetailsurlList") == 0:
            #     print('检查线程状态列表为空')
            #     os._exit(1)
            #     break
            newThreadsName = []
            for i in threading.enumerate():
                # TODO 记录正在运行的线程
                newThreadsName.append(i.getName())

            # TODO 判断有没有线程中途挂掉 如果有就重启线程
            for oldname in initThreadsName:
                if oldname in newThreadsName:
                    pass
                else:
                    print(oldname)
                    thread = threading.Thread(target=self.get_shopurl,args=oldname)
                    thread.setName(oldname)
                    thread.start()
                    print('重新启动了线程:{}'.format(oldname))
            time.sleep(20)

    def thread_start(self):
        # self.whether_task()
        global recursion_num
        thread_list = []
        init_thread_name = []  # TODO 记录线程名
        for i in range(self.thread_num):
            thread = threading.Thread(target=self.get_shopurl,args=str(i))
            # TODO 给线程赋值
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
        thread_pro = threading.Thread(target=self.checkthread,args=(init_thread_name,))
        thread_pro.start()

        for thread in thread_list:
            thread.join()

        self.thread_start()
        thread_pro.join()

if __name__ == '__main__':
    print('等待中')
    s = mainSpider()
    s.thread_start()