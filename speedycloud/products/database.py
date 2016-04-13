# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class DatabaseAPI(AbstractProductAPI):
    def list(self):
        # 数据库列表
        return self.get('/api/v1/products/databases/')
