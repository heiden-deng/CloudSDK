# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI
import json
from datetime import datetime


class CDNAPI(AbstractProductAPI):
    BASE_PATH = "/api/v1/products/cdns/"

    def _get_path(self, suffix):
        return "%s%s" % (self.BASE_PATH, suffix)

    def list(self):
        # 域名列表
        return self.get(self.BASE_PATH)

    def detail(self, id):
        '''
        CDN详细信息

        id: CDN id,

        '''
        path = self._get_path(id)
        return self.get(path)

    def modify(self, id, domain, origin_ip, cache_type):
        '''
        编辑加速域名

        id: CND id,
        domain: 域名,
        origin_ip: 源站ip,
        cache_type: 缓存策略,

        '''
        # 修改域名
        path = self._get_path("%s/modify" % str(id))
        params = {
            'domain': domain,
            'origin_ip': origin_ip,
            'cache_type': cache_type,
            'cdn_type': 'cdn_type_static',
            'cache_rules': None,
            'cdn_status': 'CNAME'
        }
        return self.post(path, {'params': json.dumps(params)})

    def pause(self, id):
        '''
        暂停域名加速

        id: CDN id

        '''
        path = self._get_path("%s/pause" % id)
        return self.post(path)

    def resume(self, id):
        '''
        回复域名加速

        id: CDN id

        '''
        path = self._get_path("%s/resume" % id)
        return self.post(path)

    def logs(self, id, start_date, end_date):
        '''
        日志文件列表

        id: CND id,
        start_date: 开始日期,
        end_date:结束日期

        '''
        params = {
            'id': id,
            'start_date': datetime.strftime(start_date, '%Y-%m-%d'),
            'end_date': datetime.strftime(end_date, '%Y-%m-%d')
        }
        path = self._get_path("%s/logs" % id)
        return self.post(path, params)

    def refresh_list(self):
        # 刷新纪录列表
        path = self._get_path("refreshes/")
        return self.get(path)

    def add_refresh(self, refresh_type, refresh_url_list):
        '''
        添加文件目录刷新

        refresh_type: 刷新类型,
        refresh_url_list: 刷新url列表, 其数据类型是list

        '''
        path = self._get_path("refreshes/add_refresh")
        params = {
            'refresh_type': refresh_type,
            'refresh_urls': json.dumps(refresh_url_list)
        }
        return self.post(path, params)

    def redo_refresh(self, refresh_id):
        '''
        重新刷新文件目录

        refresh_id: 刷新id

        '''
        path = self._get_path("refreshes/redo")
        params = {"refresh_id": refresh_id}
        return self.post(path, params)

    def delete_refresh(self, id_list):
        '''
        删除刷新纪录

        id_list: 刷新id列表, 其数据类型为list

        '''
        # 删除刷新纪录
        path = self._get_path("refreshes/delete")
        params = {"id_list": json.dumps(id_list)}
        return self.post(path, params)

    def preload_list(self):
        # 预加载纪录列表
        path = self._get_path("preload/")
        return self.get(path)

    def add_preload(self, preload_urls):
        '''
        添加问价预加载

        preload_urls: 预加载url列表, 其数据类型为list

        '''
        path = self._get_path("preload/redo")
        params = {"preload_urls": preload_urls}
        return self.post(path, params)

    def delete_preload(self, id_list):
        '''
        删除预加载纪录
        id_list: 预加载id列表, 其数据类型为list

        '''
        path = self._get_path("preload/delete")
        params = {"id_list": json.dumps(id_list)}
        return self.post(path, params)

    def set_group(self, id, group):
        '''
        设置CDN分组

        id: CDN id,
        group: 组名

        '''
        path = self._get_path("%s/group" % str(id))
        return self.post(path, {'group': group})

    def get_bandwidth(self, ids, duration=None):
        '''
        获取带宽数据

        ids: CDN id列表, 其数据类型为list

        '''
        params = {
            'ids': ids,
            'duration': duration
        }
        path = self._get_path("get_bandwidth")
        return self.post(path, params)
