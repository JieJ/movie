# encoding: utf-8
# Author: JieJ

#引入selenium中的webdriver
from selenium import webdriver
from pymongo import MongoClient
from lxml import etree
import selenium
import time
import re

url_list = ["http://movie.mtime.com/movie/search/section/#year=2000&pageIndex=1"]
phantomjs_path = "D:\\STOREAGE\\1 SOFTWARE\\phantomjs-2.1.1-windows\\bin\\phantomjs.exe"
# service_args = [
#     '--proxy=115.159.47.185:80',
#     '--proxy-type=http',
#     ]
# driver1 = webdriver.PhantomJS(executable_path=phantomjs_path,service_args=service_args)
driver1 = webdriver.PhantomJS(executable_path=phantomjs_path)          # 新建一个PhantomJS渲染程序进程

MONGODB_SERVER = 'localhost'
MONGODB_PORT = 27017
MONGODB_DB = 'movie'
client = MongoClient(MONGODB_SERVER, MONGODB_PORT)
db = client[MONGODB_DB]
collection = db['mtime_movie']

for request_url in url_list:
    year = int(re.findall('.*?year=(\d+)&.*', request_url)[0])
    start_page = int(re.findall('.*?pageIndex=(\d+)', request_url)[0])

    # request for the index(first) page, wait for 3 secs
    driver1.get(request_url)
    time.sleep(3)

    # fliping over loop
    for i in range(30):
        print u"Handling page", str(start_page+i), "......"

        # information extraction
        html_str = driver1.page_source
        html_dom = etree.HTML(html_str)

        # extract Url Link
        url_link_list = html_dom.xpath('//h3[@class="normal mt6"]/a/@href')

        # extract Chinese Movie Name
        chs_name_list = []
        chs_name_ele = html_dom.xpath('//h3[@class="normal mt6"]/a')
        for ele in chs_name_ele:
            if not ele.text is None:
                chs_name_list.append(ele.text)
            else:
                chs_name_list.append('None')

        # extract Origin Movie Name
        origin_name_list = []
        origin_name_ele = html_dom.xpath('//div[@class="td pl12 pr20"]/p/a')
        for ele in origin_name_ele:
            if not ele.text is None:
                origin_name_list.append(ele.text)
            else:
                origin_name_list.append('None')

        # extract Movie Alias
        alias_names_list = []
        alias_ele = html_dom.xpath('//div[@class="td pl12 pr20"]')
        for ele in alias_ele:
            detail_ele = ele.findall('p[@class="mt15 c_666"]')
            tmp_list = []
            for sub_ele in detail_ele:
                if not sub_ele.text is None:
                    tmp_list.append(sub_ele.text)
            alias_names = u'/'.join(tmp_list)
            alias_names = alias_names.replace('\t', '')
            if alias_names.replace(' ','')=='':
                alias_names = 'None'
            alias_names_list.append(alias_names)

        if len(chs_name_list)==0 or len(chs_name_list)!=len(origin_name_list) \
        or len(origin_name_list)!=len(alias_names_list):
            # dont match
            print "skip", start_page+i
            continue

        # 如果这里反复使用同一个变量作为插入文档的引用(i.e. 把doc = dict()写在循环外面)
        # 会引发MongoDB的DuplicateKeyError异常
        union_list = zip(url_link_list, chs_name_list, origin_name_list, alias_names_list)
        # doc_list = []
        for k in range(len(union_list)):
            item = union_list[k]
            doc = dict()
            ct = (start_page+i-1)*20+k+1
            doc['_id'] = str(year)+str(ct)
            doc['mtime_url'] = item[0]
            doc['chs_name'] = item[1]
            doc['origin_name'] = item[2]
            doc['alias'] = item[3]
            doc['year'] = year
            collection.insert_one(doc)
            # doc_list.append(doc)
        # print len(doc_list)
        # collection.insert_many(doc_list)

        # generate screen shot
        driver1.get_screenshot_as_file(str(start_page+i)+".jpg")

        try:
            next_page = driver1.find_element_by_xpath('//a[@id="key_nextpage"]')
        except selenium.common.exceptions.NoSuchElementException:
            print u"请求第", str(i), u"页失败，不存在该页"
            break
        # 因为没有使用IP代理，只有放慢速度防止被屏蔽
        time.sleep(5)
        next_page.click()
        time.sleep(2)

    driver1.close()
