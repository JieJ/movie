# encoding: utf-8
# Author: JieJ

from scrapy.spiders import Spider
from lxml import etree
from scrapy.http import Request
from movie.items import dy2018Item
import re
import time

class DySpider(Spider):
    # lastcrawl time 2016-03-12
    name = 'dy2018'
    allowed_domains = ['dy2018.com']
    collection_name = 'dy2018_movie'
    lastcrawl_time = '2000-03-01'
    crawl_dict = {
        '0':{'movie_type': 'juqingpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '1':{'movie_type':'xijupian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '2':{'movie_type':'dongzuopian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '3':{'movie_type':'aiqingpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '4':{'movie_type':'kehuanpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '5':{'movie_type':'donghuapian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '6':{'movie_type':'xuanyipian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '7':{'movie_type':'jingsongpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '8':{'movie_type':'kongbupian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '9':{'movie_type':'jilupian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '10':{'movie_type':'tongxingpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '11':{'movie_type':'yinyuegewupian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '12':{'movie_type':'zhuanjipian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '13':{'movie_type':'lishipian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '14':{'movie_type':'zhanzhengpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '15':{'movie_type':'fanzuipian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '16':{'movie_type':'qihuanpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '17':{'movie_type':'maoxianpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '18':{'movie_type':'zainanpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '19':{'movie_type':'wuxiapian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
        '20':{'movie_type':'guzhuangpian', 'current_page': 1, 'stop_flag': 0, 'time_stamp': time.mktime(time.strptime(lastcrawl_time, '%Y-%m-%d'))},
    }

    start_urls = ['http://www.dy2018.com/'+str(i)+'/' for i in range(5,6)]
    def parse(self, response):
        movie_type_code = re.findall('http://www.dy2018.com/(.*?)/.*', response.url)[0]
        detail_page_list = response.xpath('//a[@title and @class="ulink"]/@href').extract()
        time_str_list = response.xpath('//font[@color="#8F8C89"]/text()').extract()
        l1, l2 =len(detail_page_list),len(time_str_list)
        # print l1, l2
        if l1!=l2:
            print u"不匹配!"
        if l1!=0 and l1==l2:
            for page_link, time_str in zip(detail_page_list, time_str_list):
                time_str = time_str.lstrip(u'日期：').replace(' ','')
                time_stamp = time.mktime(time.strptime(time_str, '%Y-%m-%d'))
                # 如果上传时间在设定时间之前，则停止解析当前列表页，并且设置不再翻页
                print time_str
                if time_stamp < self.crawl_dict[movie_type_code]["time_stamp"]:
                    self.crawl_dict[movie_type_code]["stop_flag"] = 1
                    break
                detail_url = 'http://www.dy2018.com'+page_link
                yield Request(url=detail_url, callback=self.parse_detail,
                meta={'movie_type': self.crawl_dict[movie_type_code]['movie_type'], 'update_time': time_str})

        # 如果当前页解析的元素数量不足30
        if l1<30:
            print u"该页不足30项"
            self.crawl_dict[movie_type_code]["stop_flag"] = 1

        if self.crawl_dict[movie_type_code]["stop_flag"]==0:
            self.crawl_dict[movie_type_code]["current_page"] += 1
            next_url = 'http://www.dy2018.com/'+movie_type_code+'/index_'+str(self.crawl_dict[movie_type_code]["current_page"])+'.html'
            print "next_url", next_url
            yield Request(url=next_url)

    def parse_detail(self, response):
        movie_name = response.xpath('//title/text()').extract()[0]
        movie_name = re.findall(u'《(.*?)》', movie_name)[0]
        file_name_list = response.xpath('//td[@bgcolor="#fdfddf"]/a/text()').extract()
        download_link_list = response.xpath('//td[@bgcolor="#fdfddf"]/a/@href').extract()
        if len(file_name_list)==0 and len(download_link_list)==0:
            file_name_list = response.xpath('//td[@bgcolor="#fdfddf"]/font[@color="#ff0000"]/a/text()').extract()
            download_link_list = response.xpath('//td[@bgcolor="#fdfddf"]/font[@color="#ff0000"]/a/@href').extract()
        print len(file_name_list), len(download_link_list)
        l = dy2018Item()
        l['movie_name'] = movie_name
        l['dy2018_url'] = response.url
        l['movie_type'] = response.meta['movie_type']
        l['update_time'] = response.meta['update_time']
        if len(file_name_list)!=0 and len(file_name_list)==len(download_link_list):
            download_info = []
            for x,y in zip(file_name_list, download_link_list):
                tmp_info = {}
                tmp_info['download_filename'] = x
                tmp_info['download_link'] = y
                download_info.append(tmp_info)
            l['download_info'] = download_info
        yield l


