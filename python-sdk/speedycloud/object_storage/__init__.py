# -*- coding: utf-8 -*-
import urllib
import httplib
from datetime import datetime
import hmac
import hashlib
import socket
import time
import threading
import base64


class ProductAPIError(Exception):
    pass


class ConnectionPool:
    def __init__(self, host, protocol, size=10):
        self.protocol = protocol
        self.host = host
        self.initial_size = size
        self.size = size
        self.connections = {}
        self.lock = threading.Lock()
        self.initialize()

    def _create_connection(self):
        if self.protocol == 'http':
            return httplib.HTTPConnection(self.host)
        elif self.protocol == 'https':
            return httplib.HTTPSConnection(self.host)
        else:
            raise ProductAPIError(
                'Do not support protocol: %s' % self.protocol)

    def initialize(self):
        for i in range(self.size):
            conn = self._create_connection()
            self.connections[conn] = False

    def get_connection(self):
        self.lock.acquire()
        try:
            for c, used in self.connections.items():
                if not used:
                    self.connections[c] = True
                    return c
            # If we got there, we should expand this pool
            new_conn = self._create_connection()
            self.connections[new_conn] = True
            self.size += 1
            return new_conn
        finally:
            self.lock.release()

    def put_connection(self, connection):
        self.lock.acquire()
        try:
            self.connections[connection] = False
            self.__clean()
        finally:
            self.lock.release()

    def __clean(self):
        need_clean = len(self.connections) - self.initial_size
        if need_clean > 0:
            cleaned = need_clean
            for c, used in self.connections.items():
                if cleaned <= 0:
                    break
                if not used:
                    c.close()
                    del self.connections[c]
                    cleaned -= 1
                    self.size -= 1

    def reconnect(self, connection):
        connection.close()

    @classmethod
    def get_instance(cls, host, protocol, size=10):
        if not hasattr(cls, "__POOL_INSTANCE__"):
            cls.__POOL_INSTANCE__ = {}
        cache_key = "%s-%s" % (host, protocol)
        if cache_key not in cls.__POOL_INSTANCE__:
            cls.__POOL_INSTANCE__[cache_key] = ConnectionPool(
                host, protocol, size)
        return cls.__POOL_INSTANCE__[cache_key]


class AbstractProductAPI(object):
    def __init__(self, access_key, secret_key, protocol='http'):
        self.host = 'osc.speedycloud.net'
        self.access_key = access_key
        self.secret_key = secret_key
        self.protocol = protocol
        self.response_header = []
        self.pool = ConnectionPool.get_instance(
            self.host, protocol)

    def _encode_params(self, params):
        if params is None:
            return ''
        return urllib.urlencode(params)

    def _generate_headers(self, method, path, params):
        request_date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        sign = self.create_sign(method, path, params)
        authorization = "AWS" + " " + str(self.access_key) + ":" + sign
        header_data = {
            'Date': request_date,
            'Authorization': authorization,
        }
        if 'x-amz-acl' in params:
            header_data['x-amz-acl'] = params['x-amz-acl']
        if 'content_length' in params:
            header_data['Content-Length'] = params['content_length']
        if 'content_type' in params:
            header_data['Content-Type'] = params['content_type']
        return header_data

    def create_sign(self, method, path, params):
        canonicalized_amz_headers = ''
        if 'x-amz-acl' in params:
            canonicalized_amz_headers = 'x-amz-acl:%s' % params['x-amz-acl']
        sign_str = self.create_sign_str(
            http_method=method,
            url=path,
            content_md5=params.get('content_md5', ''),
            content_type=params.get('content_type', ''),
            params=params.get('params', ''),
            canonicalized_amz_headers=canonicalized_amz_headers,
        )
        sign = hmac.new(self.secret_key, sign_str, digestmod=hashlib.sha1)
        return base64.b64encode(sign.digest())

    def create_sign_str(self, **params):
        http_header_date = str(
            datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        )

        sign_param_list = [params['http_method'], params[
            'content_md5'], params['content_type'], http_header_date]
        if params['canonicalized_amz_headers']:
            sign_param_list.append(params['canonicalized_amz_headers'])
        sign_param_list.append(params['url'])
        return '\n'.join(sign_param_list)

    def request(self, method, path, data, params):
        last_exception = None
        connection = self.pool.get_connection()
        try:
            # for now we do 5 times retry to ignore more connection related
            # exceptions.
            for i in range(5):
                try:
                    if method == 'GET':
                        req_path = path
                        connection.request(
                            method, req_path, data, self._generate_headers(method, path, params))
                    elif method == 'PUT':
                        req_path = path
                        connection.request(
                            method, req_path, data, self._generate_headers(method, path, params))

                    elif method == 'DELETE':
                        req_path = path
                        connection.request(
                            method, req_path, data, self._generate_headers(method, path, params))
                    elif method == 'POST':
                        req_path = path
                        connection.request(
                            method, req_path, data, self._generate_headers(method, path, params))
                    resp = connection.getresponse()
                    self.response_header = resp.getheaders()
                    return resp.read()
                except httplib.HTTPException, e:
                    self.pool.reconnect(connection)
                    last_exception = e
                except socket.error, e:
                    self.pool.reconnect(connection)
                    if 1 < i < 4:
                        time.sleep(1)
                    last_exception = e
            # if we got there, we got some connection related problems so we should rise this
            # Exception to upper function. If we do nothing here this function will return None
            # instead so we may got some error else.
            raise last_exception
        finally:
            self.pool.put_connection(connection)

    def post(self, path, data=None, params={}):
        return self.request('POST', path, data, params)

    def get(self, path, data=None, params={}):
        return self.request('GET', path, data, params)

    def put(self, path, data=None, params={}):
        return self.request('PUT', path, data, params)

    def upload_big_data_put(self, path, data=None, params={}):
        return self.request('PUT', path, data, params), self.response_header

    def delete(self, path, data=None, params={}):
        return self.request('DELETE', path, data, params)
