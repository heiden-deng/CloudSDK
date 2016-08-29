# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class CloudVolumeAPI(AbstractProductAPI):
    BASE_PATH = '/api/v1/products/cloud_volumes/'

    def _get_path(self, suffix):
        return "%s%s" % (self.BASE_PATH, suffix)

    def provision(self, available_zone, size):
        '''
        创建云硬盘

        available_zone: 可用数据中心
        size: 云硬盘容量

        '''
        path = self._get_path("provision")
        params = {
            'az': available_zone,
            'size': size,
        }
        return self.post(path, params)

    def list(self):
        # 云硬盘列表
        return self.get(self.BASE_PATH)

    def detail(self, cloud_volume_id):
        '''
        云硬盘详细信息

        cloud_volume_id: 云硬盘id

        '''
        path = self._get_path(str(cloud_volume_id))
        return self.get(path)

    def snapshots(self, cloud_volume_id):
        '''
        云硬盘快照列表

        cloud_volume_id: 云硬盘id

        '''
        path = self._get_path("%s/snapshots" % str(cloud_volume_id))
        return self.get(path)

    def jobs(self, cloud_volume_id):
        '''
        云硬盘任务列表

        cloud_volume_id: 云硬盘id

        '''
        path = self._get_path("%s/jobs" % str(cloud_volume_id))
        return self.get(path)

    def set_tag(self, cloud_volume_id, tag):
        '''
        为云硬盘设置标签

        cloud_volume_id: 云硬盘id
        tag: 标签名

        '''
        path = self._get_path("%s/tag" % str(cloud_volume_id))
        return self.post(path, {'tag': tag})

    def set_alias(self, cloud_volume_id, alias):
        '''
        为云硬盘设置别名

        cloud_volume_id: 云硬盘id
        alias: 别名

        '''
        path = self._get_path("%s/alias" % str(cloud_volume_id))
        return self.post(path, {'alias': alias})

    def set_group(self, cloud_volume_id, group):
        '''
        为云硬盘设置分组

        cloud_volume_id: 云硬盘id
        group: 组名

        '''
        path = self._get_path("%s/group" % str(cloud_volume_id))
        return self.post(path, {'group': group})

    def create_snapshot(self, cloud_volume_id):
        '''
        创建快照

        cloud_volume_id: 云硬盘id

        '''
        path = self._get_path("%s/snapshot" % str(cloud_volume_id))
        return self.post(path)

    def rollback_snapshot(self, cloud_volume_id, snapshot):
        '''
        回滚快照

        cloud_volume_id: 云硬盘id
        snapshot: 快照名

        '''
        path = self._get_path("%s/snapshots/%s/rollback" % (str(cloud_volume_id), snapshot))
        return self.post(path)

    def delete_snapshot(self, cloud_volume_id, snapshot):
        '''
        删除快照

        cloud_volume_id: 云硬盘id
        snapshot: 快照名

        '''
        path = self._get_path("%s/snapshots/%s/delete" % (str(cloud_volume_id), snapshot))
        return self.post(path)

    def get_available_zone(self):
        # 获得可用数据中心
        azs = self.get('/api/v1/availability_zones/names')
        return azs