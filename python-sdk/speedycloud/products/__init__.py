# -*- coding: utf-8 -*-
import urllib
import httplib
from datetime import datetime
import hmac
import hashlib
import socket
import json
import time
import threading


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
            raise ProductAPIError('Do not support protocol: %s' % self.protocol)

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
            cls.__POOL_INSTANCE__[cache_key] = ConnectionPool(host, protocol, size)
        return cls.__POOL_INSTANCE__[cache_key]


class AbstractProductAPI(object):
    # api_keys := {
    #   'access_key': ACCESS_KEY,
    #   'secret_key': SECRET_KEY,
    # }
    def __init__(self, access_key, secret_key, protocol='http'):
        self.host = 'api.speedycloud.cn'
        self.access_key = access_key
        self.secret_key = secret_key
        self.protocol = protocol
        self.pool = ConnectionPool.get_instance('api.speedycloud.cn', protocol)

    def _encode_params(self, params):
        if params is None:
            return ''
        return urllib.urlencode(params)

    def _generate_headers(self, method, path):
        request_date = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        sign = hmac.new(self.secret_key, method.upper() + "\n", hashlib.sha1)
        sign.update(path + "\n")
        sign.update(request_date + "\n")
        authorization = "%s,%s" % (self.access_key, sign.hexdigest())
        return {
            'Date': request_date,
            'Authorization': authorization,
        }

    def request(self, method, path, params):
        last_exception = None
        query_string = self._encode_params(params)
        connection = self.pool.get_connection()
        try:
            # for now we do 5 times retry to ignore more connection related exceptions.
            for i in range(5):
                try:
                    if method == 'GET':
                        req_path = path
                        if query_string != '':
                            req_path = path + "?" + query_string
                        connection.request(method, req_path, None, self._generate_headers(method, path))
                    else:
                        headers = self._generate_headers(method, path)
                        headers['Content-Type'] = 'application/x-www-form-urlencoded'
                        connection.request(method, path, query_string, headers)
                    resp = connection.getresponse()
                    return self.process_response(resp.status, resp.read())
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

    def process_response(self, status_code, response):
        if status_code == 200:
            return json.loads(response)
        else:
            try:
                error_message = json.loads(response)['error']
            except Exception, e:
                error_message = response
            raise ProductAPIError(error_message)

    def post(self, path, params=None):
        return self.request('POST', path, params)

    def get(self, path, params=None):
        return self.request('GET', path, params)
