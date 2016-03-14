# encoding: utf-8
# Author: JieJ

from scrapy.spiders import Spider
from lxml import etree
from scrapy.http import Request
from movie.items import ys66Item
import re
import time

class WanSpider(Spider):
    # lastcrawl time 2016-03-12
    name = "66ys"
    allowed_domains = ["66ys.tv"]
    collection_name = '66ys_movie'
    # movie_type_list = ['xijupian', 'dongzuopian', 'aiqingpian', 'kehuanpian', 'kongbupian', 'zhanzhengpian', 'julupian', 'bd']
    # movie_type_list = ['xijupian','dongzuopian', 'aiqingpian', 'kehuanpian', 'kongbupian', 'zhanzhengpian', 'julupian']
    # movie_type_list = ['xijupian']
    movie_type_list = ['dmq']
    crawl_dict = {
    "xijupian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2016-02-25', '%Y-%m-%d'))},
    "dongzuopian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-02-25', '%Y-%m-%d'))},
    "aiqingpian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-01-01', '%Y-%m-%d'))},
    "kehuanpian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-01-01', '%Y-%m-%d'))},
    "kongbupian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-01-01', '%Y-%m-%d'))},
    "zhanzhengpian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-01-01', '%Y-%m-%d'))},
    "jilupian": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-01-01', '%Y-%m-%d'))},
    "bd": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2000-03-01', '%Y-%m-%d'))},
    "dmq": {"current_page": 1, "stop_flag": 0, "time_stamp": time.mktime(time.strptime('2016-03-01', '%Y-%m-%d'))},
    }
    # start_urls = ["http://www.66ys.tv/"+movie_type+'/index.html' for movie_type in movie_type_list]
    start_urls = ["http://www.66ys.tv/"+movie_type+'/index.htm' for movie_type in movie_type_list]

    def parse(self, response):
        tmp_type = re.findall('http://www.66ys.tv/(.*?)/.*', response.url)[0]
        detail_page_list = response.xpath('//div[@class="listBox"]/ul/li/div[@class="listInfo"]/h3/a/@href').extract()
        time_str_list = response.xpath('//div[@class="listBox"]/ul/li/div[@class="listInfo"]/p[3]/text()').extract()
        l1, l2 =len(detail_page_list),len(time_str_list)
        if l1!=0 and l1==l2:
            for page_link, time_str in zip(detail_page_list, time_str_list):
                time_str = time_str.lstrip(u'时间：')
                time_stamp = time.mktime(time.strptime(time_str, '%Y-%m-%d'))
                # 如果上传时间在设定时间之前，则停止解析当前列表页，并且设置不再翻页
                if time_stamp < self.crawl_dict[tmp_type]["time_stamp"]:
                    self.crawl_dict[tmp_type]["stop_flag"] = 1
                    break
                if page_link.startswith("http://www.66ys.tv"):
                    yield Request(url=page_link, callback=self.parse_detail)
            # 如果当前页解析的元素数量不足20
            if l1<20:
                self.crawl_dict[tmp_type]["stop_flag"] = 1

            if self.crawl_dict[tmp_type]["stop_flag"]==0:
                self.crawl_dict[tmp_type]["current_page"] += 1
                if tmp_type=='bd' or tmp_type=='dmq':
                    next_url = 'http://www.66ys.tv/'+tmp_type+'/index_'+str(self.crawl_dict[tmp_type]["current_page"])+'.htm'
                else:
                    next_url = 'http://www.66ys.tv/'+tmp_type+'/index_'+str(self.crawl_dict[tmp_type]["current_page"])+'.html'
                yield Request(url=next_url)
        else:
            # 异常情况Logging
            pass

    def parse_detail(self, response):
        tmp_type = re.findall('http://www.66ys.tv/(.*?)/.*', response.url)[0]
        movie_name_list = response.xpath('//div[@class="contentinfo"]/h1/text()').extract()
        download_links = response.css('div#text table tbody tr td a::attr(href)').extract()
        download_filenames = response.css('div#text table tbody tr td a::text').extract()

        # 使用正则表达式在网页源码中匹配网盘下载信息
        html_body = response.body.decode('gbk','ignore').encode('utf8','ignore')
        wangpan_patt = '网盘[\w\W]*?>(.*?)</a> 密码：(.*?)</td>'
        wangpan_lst = re.findall(wangpan_patt, html_body)

        l = ys66Item()
        l['ys66_url'] = response.url
        l['movie_type'] = tmp_type
        if len(movie_name_list) > 0:
            movie_name = movie_name_list[0].strip().strip(u'《').strip(u'》')
            l['movie_name'] = movie_name

        # 如果下载链接和文件名数量匹配且大于0
        if len(download_filenames)==len(download_links) and len(download_filenames)>0:
            download_info = []
            for x,y in zip(download_filenames, download_links):
                if u'pan.baidu.com' in x:
                    continue
                if u'在线观看' in x:
                    l['online_url'] = y
                    continue
                tmp_info = {}
                tmp_info['download_filename'] = x
                tmp_info['download_link'] = y
                download_info.append(tmp_info)
            l['download_info'] = download_info

        # 如果存在网盘下载链接
        if len(wangpan_lst) > 0:
            wangpan_info = []
            for pan in wangpan_lst:
                tmp_pan_info = {}
                tmp_pan_info['download_link'] = pan[0]
                tmp_pan_info['wangpan_pwd'] = pan[1]
                wangpan_info.append(tmp_pan_info)
            l['wangpan_info'] = wangpan_info

        yield l


