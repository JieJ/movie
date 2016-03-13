# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ys66Item(scrapy.Item):
    _id = scrapy.Field()
    ys66_url = scrapy.Field()
    movie_name = scrapy.Field()
    movie_type = scrapy.Field()
    download_info = scrapy.Field()
    wangpan_info = scrapy.Field()
    online_url = scrapy.Field()

class mtimeItem(scrapy.Item):
    mtime_url = scrapy.Field()
    movie_name = scrapy.Field()
    other_names = scrapy.Field()
    year = scrapy.Field()
