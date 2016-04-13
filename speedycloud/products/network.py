# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class NetworkAPI(AbstractProductAPI):
    BASE_PATH = "/api/v1/products/networks/"

    def _get_path(self, suffix):
        return "%s%s" % (self.BASE_PATH, suffix)

    def create(self, available_zone):
        '''
        创建网络

        available_zone: 可用数据中心

        '''
        path = self._get_path("create")
        return self.post(path, {'az': available_zone})

    def list(self):
        # 网络列表
        return self.get(self.BASE_PATH)

    def detail(self, network_id):
        '''
        私有网络信息

        network_id: 私有网络id

        '''
        path = self._get_path(network_id)
        return self.get(path)

    def delete(self, network_id):
        '''
        删除私有网络

        network_id: 私有网络id

        '''
        path = self._get_path("%s/delete" % str(network_id))
        return self.post(path)

    def set_alias(self, network_id, alias):
        '''
        为网络设置别名

        network_id: 私有网络id,
        alias: 别名

        '''
        path = self._get_path("%s/alias" % str(network_id))
        return self.post(path, {'alias': alias})

    def set_group(self, network_id, group):
        '''
        为网络设置分组

        network_id: 私有网络id,
        group: 组名

        '''
        path = self._get_path("%s/group" % str(network_id))
        return self.post(path, {'group': group})

    def groups(self):
        # 私有网络分组列表
        path = self._get_path('groups')
        return self.get(path)

    def create_database(self, available_zone, isp, image, memory, disk, bandwidth):
        '''
        创建数据库

        available_zone: 可用数据中心,
        memory: 内存大小,
        disk: 硬盘大小,
        bandwidth: 带宽大小,
        isp: 数据中心支持的运营商
        image: 数据库镜像

        '''
        configurations = {
            'az': available_zone,
            'memory': memory,
            'disk': disk,
            'bandwidth': bandwidth,
            'isp': isp,
            'image': image
        }
        return self.post("/api/v1/products/databases/provision", configurations)

    def create_cache(self, available_zone, isp, image, memory, disk, bandwidth):
        '''
        创建缓存
        
        available_zone: 可用数据中心,
        memory: 内存大小,
        disk: 硬盘大小,
        bandwidth: 带宽大小,
        isp: 数据中心支持的网络运营商,
        image: 数据库镜像

        '''
        configurations = {
            'az': available_zone,
            'memory': memory,
            'disk': disk,
            'bandwidth': bandwidth,
            'isp': isp,
            'image': image,
        }
        return self.post("/api/v1/products/caches/provision", configurations)

    def get_available_zone(self):
        # 获得可用数据中心
        azs = self.get('/api/v1/availability_zones/names')
        return azs

    def get_support_isps(self, az_name):
        # 获得数据中心支持运营商
        azs = self.get('/api/v1/availability_zones/')
        for az in azs:
            if az['name'] == az_name:
                return az['support_isps']
        return ''

    def get_database_images(self):
        # 获得数据库镜像列表
        images = self.get('/api/v1/products/database_images_names/')
        return images

    def get_images_for_database(self):
        # 获得数据库镜像列表
        images = self.get_database_images()
        imgs = []
        for image in images:
            if image['type'] == 'MySQL' or image['type'] == 'MongoDB':
                imgs.append(image['name'])
        return imgs

    def get_images_for_cache(self):
        # 获得缓存镜像列表
        images = self.get_database_images()
        imgs = []
        for image in images:
            if image['type'] != 'MySQL' and image['type'] != 'MongoDB':
                imgs.append(image['name'])
        return imgs
