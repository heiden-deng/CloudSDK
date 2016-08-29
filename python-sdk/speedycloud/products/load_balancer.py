# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class LoadBalancerAPI(AbstractProductAPI):
    BASE_PATH = '/api/v1/products/load_balancers/'

    def _get_path(self, suffix):
        return "%s%s" % (self.BASE_PATH, suffix)

    def list(self):
        # 负载均衡列表
        return self.post(self.BASE_PATH)

    def create_load_balance(self, available_zone, isp, bandwidth):
        '''
        创建负载均衡

        available_zone: 可用数据中心
        isp: 数据中心支持的运营商
        bandwidth: 带宽

        '''
        configurations = {
            'az': available_zone,
            'isp': isp,
            'bandwidth': bandwidth,
        }
        path = self._get_path("provision")
        return self.post(path, configurations)

    def add_backend_cloud_server(self, load_balancer_id, cloud_server_id, weight, ip_address):
        '''
        添加后端云主机

        load_balancer_id: 负载均衡id
        cloud_server_id: 云主机id
        weight: 权重
        ip_address: ip地址

        '''
        params = {
            'cloud_server_id': cloud_server_id,
            'weight': weight,
            'ip_address': ip_address,
        }
        path = self._get_path(str(load_balancer_id) + '/backends/add')
        return self.post(path, params)

    def add_backend_database(self, load_balancer_id, database_id, weight, ip_address):
        '''
        添加后端数据库

        load_balancer_id: 负载均衡id
        database_id: 数据库id
        weight: 权重
        ip_address: ip地址

        '''
        params = {
            'database_id': database_id,
            'weight': weight,
            'ip_address': ip_address,
        }
        path = self._get_path(str(load_balancer_id) + '/backends/add')
        return self.post(path, params)

    def add_backend_cache(self, load_balancer_id, cache_id, weight, ip_address):
        '''
        添加后端缓存

        load_balancer_id: 负载均衡id
        cache_id: 缓存id
        weight: 权重
        ip_address: ip地址

        '''
        params = {
            'database_id': cache_id,
            'weight': weight,
            'ip_address': ip_address,
        }
        path = self._get_path(str(load_balancer_id) + '/backends/add')
        return self.post(path, params)

    def update_backend(self, load_balancer_id, back_id, weight, ip_address):
        '''
        更新后端

        load_balancer_id: 负载均衡id
        back_id: 后端id
        weight: 权重
        ip_address: ip地址

        '''
        path = self._get_path(str(load_balancer_id) + '/backends/' + str(back_id) + '/update')
        params = {
            'weight': weight,
            'ip_address': ip_address,
        }
        return self.post(path, params)

    def delete_backend(self, load_balancer_id, back_id):
        '''
        删除后端

        load_balancer_id: 负载均衡id
        back_id: 后端id

        '''
        path = self._get_path(str(load_balancer_id) + '/backends/' + str(back_id) + '/delete')
        return self.post(path)

    def add_application(self, load_balancer_id, frontend, backend, protocol, strategy, check_interval, rise_times,
                        fall_times):
        '''
        添加应用

        load_balancer_id: 负载均衡id
        frontend: 前端端口
        backend: 后端端口
        protocol: 协议
        strategy: 负载均衡策略
        check_interval: 健康检查间隔
        rise_times: 下线监测阀值
        fall_times: 在线监测阀值

        '''
        params = {
            'frontend': frontend,
            'backend': backend,
            'protocol': protocol,
            'strategy': strategy,
            'check_interval': check_interval,
            'rise_times': rise_times,
            'fall_times': fall_times
        }
        path = self._get_path(str(load_balancer_id) + '/applications/add')
        return self.post(path, params)

    def detail(self, load_balancer_id):
        '''
        负载均衡详细信息

        load_balancer_id: 负载均衡id

        '''
        path = self._get_path(str(load_balancer_id))
        return self.post(path)

    def update_application(self, load_balancer_id, application_id, frontend, backend, protocol, strategy,
                           check_interval, rise_times, fall_times):
        '''
        更新应用

        load_balancer_id: 负载均衡id
        application_id: 应用id
        frontend: 前端端口
        backend: 后端端口
        protocol: 协议
        strategy: 负载均衡策略
        check_interval: 健康检查间隔
        rise_times: 下线监测阀值
        fall_times: 在线监测阀值

        '''
        params = {
            'frontend': frontend,
            'backend': backend,
            'protocol': protocol,
            'strategy': strategy,
            'check_interval': check_interval,
            'rise_times': rise_times,
            'fall_times': fall_times
        }
        path = self._get_path(str(load_balancer_id) + '/applications/' + str(application_id) + '/update')
        return self.post(path, params)

    def delete_application(self, load_balancer_id, application_id):
        '''
        删除应用

        load_balancer_id: 负载均衡id
        application_id: 应用id

        '''
        path = self._get_path(str(load_balancer_id) + '/applications/' + str(application_id) + '/delete')
        return self.post(path)

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