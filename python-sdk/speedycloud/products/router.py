# -*- coding: utf-8 -*-
from speedycloud.products import AbstractProductAPI


class RouterAPI(AbstractProductAPI):
    BASE_PATH = "/api/v1/products/routers"

    def _get_path(self, suffix):
        return "%s/%s" % (self.BASE_PATH, suffix)

    def list(self):
        # 路由器列表
        path = self.BASE_PATH
        return self.get(path)

    def detail(self, id):
        '''
        路由器详细信息

        id: 路由器id

        '''
        path = self._get_path(str(id))
        return self.post(path)

    def edit_nat_role(self, id, port, protocol, target_ip, target_port):
        '''
        编辑NAT规则

        id: 路由器id,
        port: NAT端口号,
        protocol: NAT协议,
        target_ip: 目标ip,
        target_port: 目标端口

        '''
        params = {
            'port': port,
            'protocol': protocol,
            'target_ip': target_ip,
            'target_port': target_port
        }
        path = self._get_path("%s/edit_nat" % str(id))
        return self.post(path, params)

    def delete_nat_role(self, id, port, protocol):
        '''
        删除NAT规则

        port: NAT端口号，
        protocol: NAT协议

        '''
        params = {
            'port': port,
            'protocol': protocol
        }
        path = self._get_path("%s/delete_nat" % str(id))
        return self.post(path, params)

    def create_nat_role(self, id, port, protocol, target_ip, target_port):
        '''
        添加NAT规则

        id: 路由器id,
        port: NAT端口号,
        protocol: NAT协议,
        target_ip: 目标ip,
        target_port: 目标端口

        '''
        params = {
            'port': port,
            'protocol': protocol,
            'target_ip': target_ip,
            'target_port': target_port
        }
        path = self._get_path("%s/create_nat" % str(id))
        return self.post(path, params)

    def set_group(self, id, group_name):
        '''
        设置分组

        id: 路由器id,
        group_name: 分组名

        '''
        path = self._get_path("%s/set_group" % str(id))
        return self.post(path, {"group": group_name})

    def groups(self):
        # 列出所有分组
        path = self._get_path('groups')
        return self.get(path)

    def set_alias(self, id, alias):
        '''
        设置别名
        id: 路由器id,
        alias: 别名

        '''
        path = self._get_path("%s/alias" % str(id))
        return self.post(path, {'alias': alias})

    def stop(self, id):
        '''
        停止路由器

        id: 路由器id

        '''
        path = self._get_path("%s/stop" % str(id))
        return self.post(path)

    def start(self, id):
        '''
        开启路由器
        
        id: 路由器id

        '''
        path = self._get_path("%s/start" % str(id))
        return self.post(path)

    def jobs(self, id):
        '''
        路由器任务列表

        id: 路由器id

        '''
        path = self._get_path("%s/jobs" % str(id))
        return self.get(path)

    def support_features(self, id):
        '''
        路由器支持的特性
        
        id: 路由器id

        '''
        path = self._get_path("%s/support_features" % str(id))
        return self.get(path)

    def join(self, id, network, ip, mask):
        '''
        加入私有网络

        id: 路由器id,
        network: 私有网路名,
        ip: ip地址,
        mask: 网关

        '''
        netcfg = {
            'name': network,
            'ip': ip,
            'mask': mask
        }
        path = self._get_path("%s/join/%s" % (str(id), network))
        return self.post(path, netcfg)

    def toggle_private_network(self, id):
        '''
        默认内网开关

        id: 路由器id

        '''
        path = self._get_path("%s/toggle_private_network" % str(id))
        return self.post(path)

    def reload(self, id):
        '''
        重新加载路由器

        id: 路由器id

        '''
        path = self._get_path("%s/reload" % str(id))
        return self.post(path)

    def rejoin(self, id, network, ip, mask):
        '''
        重新加入私有网络

        id: 路由器id,
        network: 私有网络名
        ip: ip地址，
        mask: 网关

        '''
        netcfg = {
            'name': network,
            'ip': ip,
            'mask': mask
        }
        path = self._get_path("%s/rejoin/%s" % (str(id), network))
        return self.post(path, netcfg)

    def leave(self, id, network):
        '''
        离开私有网络

        id: 路由器id,
        network: 私有网络名

        '''
        path = self._get_path("%s/leave/%s" % (str(id), network))
        return self.post(path)
