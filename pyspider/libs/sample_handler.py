#!/usr/bin/env python
# -*- encoding: utf-8 -*-
# Created on __DATE__
# Project: __PROJECT_NAME__

from pyspider.libs.base_handler import *


class Handler(BaseHandler):
    crawl_config = {
    }

    @every(minutes=5)
    def on_start(self):
        self.crawl('__START_URL__', callback=self.index_page)

    @config(age=5 * 60)
    def index_page(self, response):
        for each in response.doc('a[href^="http"]').items():
            self.crawl(each.find('a').attr.href, callback=self.detail_page, skip_fetcher=True, save={'title': each.find('a').attr.title, 'publish_date': each.find('span').text()})

    @config(priority=2)
    def detail_page(self, response):
        return {
            "title": response.save['title'],
            "publish_date": response.save['publish_date'],
        }
