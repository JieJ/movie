# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

from scrapy.conf import settings
from scrapy.exceptions import DropItem
from pymongo import MongoClient
from pymongo.bulk import BulkOperationBuilder
from pymongo.errors import BulkWriteError
import scrapy
import os

class MoviePipeline(object):

    def __init__(self):
        server = settings['MONGODB_SERVER']
        port = settings['MONGODB_PORT']
        dbName = settings['MONGODB_DB']
        self.client = MongoClient(server, port)
        self.db = self.client[dbName]
        self.bulk_max = 500         # 操作数上限
        # self.ids_seen = set()     # 这个集合可以用来过滤掉重复的item

    # def from_crawler(cls, crawler):
    #     pass

    def open_spider(self, spider):
        self.collection = self.db[spider.collection_name]
        self.bulk = BulkOperationBuilder(self.collection, ordered=False)    #建立一个并行的bulk对象
        self.bulk_ct = 0     # 这个变量用来记录bulk中的操作数量，满足一定值以后就执行一次


    def close_spider(self, spider):
        try:
            self.bulk.execute()
        except BulkWriteError:
            pass
        self.client.close()

    def process_item(self, item, spider):
        if spider.name=='66ys':
            store_item = dict(item)
            if store_item.has_key('download_info') or store_item.has_key('wangpan_info'):
                self.bulk.insert(store_item)
                self.bulk_ct += 1
                if self.bulk_ct >= self.bulk_max:
                    self.bulk.execute()
                    self.bulk = BulkOperationBuilder(self.collection, ordered=False)    #重新新建1个并行的bulk对象实例
                    self.bulk_ct = 0
                # return item
            else:
                ss = store_item['movie_name']+'\t'+store_item['ys66_url']
                spider.logger.info(ss)
        elif spider.name=='dy2018':
            # return item
            store_item = dict(item)
            if store_item.has_key('download_info'):
                self.bulk.insert(store_item)
                self.bulk_ct += 1
                if self.bulk_ct >= self.bulk_max:
                    self.bulk.execute()
                    self.bulk = BulkOperationBuilder(self.collection, ordered=False)    #重新新建1个并行的bulk对象实例
                    self.bulk_ct = 0
            else:
                ss = store_item['movie_name']+'\t'+store_item['dy2018_url']
                spider.logger.info(ss)
        else:
            pass
