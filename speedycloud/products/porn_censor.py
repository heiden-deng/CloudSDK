# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class CensorAPI(AbstractProductAPI):
    BASE_PATH = '/api/v1/porn_censor'

    def censor(self, url):
        return self.post(self.BASE_PATH, {'url': url})
