# -*- coding: utf-8 -*-
# Copyright (c) 2013 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy of
# the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations under
# the License.

import snakebite.protobuf.ClientNamenodeProtocol_pb2 as client_proto
import snakebite.glob as glob
from snakebite.base_client import BaseClient
from snakebite.platformutils import get_current_username
from snakebite.channel import DataXceiverChannel
from snakebite.config import HDFSConfig
from snakebite.errors import (
    ConnectionFailureException,
    DirectoryException,
    FileAlreadyExistsException,
    FileException,
    FileNotFoundException,
    InvalidInputException,
    OutOfNNException,
    RequestError,
    FatalException, TransientException)
from snakebite.namenode import Namenode
from snakebite.service import RpcService

import Queue
import zlib
import bz2
import logging
import os
import os.path
import posixpath
import fnmatch
import inspect
import socket
import errno
import time
import re
import sys
import random

if sys.version_info[0] == 3:
    long = int

log = logging.getLogger(__name__)


class Client(BaseClient):
    """ A pure python HDFS client.

    **Example:**

    >>> from snakebite.client import Client
    >>> client = Client("localhost", 8020, use_trash=False)
    >>> for x in client.ls(['/']):
    ...     print x

    .. warning::

        Many methods return generators, which mean they need to be consumed to execute! Documentation will explicitly
        specify which methods return generators.

    .. note::
        ``paths`` parameters in methods are often passed as lists, since operations can work on multiple
        paths.

    .. note::
        Parameters like ``include_children`` and ``recurse`` are not used
        when paths contain globs.

    .. note::
        Different Hadoop distributions use different protocol versions. Snakebite defaults to 9, but this can be set by passing
        in the ``hadoop_version`` parameter to the constructor.
    """

    def __init__(self, host, port=Namenode.DEFAULT_PORT, hadoop_version=Namenode.DEFAULT_VERSION,
                 use_trash=False, effective_user=None, use_sasl=False, hdfs_namenode_principal=None,
                 sock_connect_timeout=10000, sock_request_timeout=10000, use_datanode_hostname=False):
        """
        :param host: Hostname or IP address of the NameNode
        :type host: string
        :param port: RPC Port of the NameNode
        :type port: int
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        :param use_trash: Use a trash when removing files.
        :type use_trash: boolean
        :param effective_user: Effective user for the HDFS operations (default: None - current user)
        :type effective_user: string
        :param use_sasl: Use SASL authentication or not
        :type use_sasl: boolean
        :param hdfs_namenode_principal: Kerberos principal to use for HDFS
        :type hdfs_namenode_principal: string
        :param sock_connect_timeout: Socket connection timeout in seconds
        :type sock_connect_timeout: int
        :param sock_request_timeout: Request timeout in seconds
        :type sock_request_timeout: int
        :param use_datanode_hostname: Use hostname instead of IP address to commuicate with datanodes
        :type use_datanode_hostname: boolean
        """

        super(Client, self).__init__(hadoop_version=hadoop_version,
                                     use_trash=use_trash,
                                     use_sasl=use_sasl,
                                     hdfs_namenode_principal=hdfs_namenode_principal,
                                     use_datanode_hostname=use_datanode_hostname)
        self.host = host
        self.port = port

        self.service_stub_class = client_proto.ClientNamenodeProtocol_Stub
        self.service = RpcService(self.service_stub_class, self.port, self.host, hadoop_version,
                                  effective_user,self.use_sasl, self.hdfs_namenode_principal,
                                  sock_connect_timeout, sock_request_timeout)

        log.debug("Created client for %s:%s with trash=%s and sasl=%s" % (host, port, use_trash, use_sasl))

    def _get_service(self):
        return self.service


class HAClient(Client):
    ''' Snakebite client with support for High Availability

    HAClient is fully backwards compatible with the vanilla Client and can be used for a non HA cluster as well.

    **Example:**

    >>> from snakebite.client import HAClient
    >>> from snakebite.namenode import Namenode
    >>> n1 = Namenode("namenode1.mydomain", 8020)
    >>> n2 = Namenode("namenode2.mydomain", 8020)
    >>> client = HAClient([n1, n2], use_trash=True)
    >>> for x in client.ls(['/']):
    ...     print x

    .. note::
        Different Hadoop distributions use different protocol versions. Snakebite defaults to 9, but this can be set by passing
        in the ``version`` parameter to the Namenode class constructor.
    '''

    @classmethod
    def _wrap_methods(cls):
        # Add HA support to all public Client methods, but only do this when we haven't done this before
        for name, meth in inspect.getmembers(cls, inspect.ismethod):
            if not name.startswith("_"): # Only public methods
                if inspect.isgeneratorfunction(meth):
                    setattr(cls, name, cls._ha_gen_method(meth))
                else:
                    setattr(cls, name, cls._ha_return_method(meth))

    def _reset_retries(self):
        log.debug("Resetting retries and failovers")
        self.failovers = 0
        self.retries = 0

    def __init__(self, namenodes, use_trash=False, effective_user=None, use_sasl=False, hdfs_namenode_principal=None,
                 max_failovers=15, max_retries=10, base_sleep=500, max_sleep=15000, sock_connect_timeout=10000,
                 sock_request_timeout=10000, use_datanode_hostname=False):
        '''
        :param namenodes: Set of namenodes for HA setup
        :type namenodes: list
        :param use_trash: Use a trash when removing files.
        :type use_trash: boolean
        :param effective_user: Effective user for the HDFS operations (default: None - current user)
        :type effective_user: string
        :param use_sasl: Use SASL authentication or not
        :type use_sasl: boolean
        :param hdfs_namenode_principal: Kerberos principal to use for HDFS
        :type hdfs_namenode_principal: string
        :param max_retries: Number of failovers in case of connection issues
        :type max_retries: int
        :param max_retries: Max number of retries for failures
        :type max_retries: int
        :param base_sleep: Base sleep time for retries in milliseconds
        :type base_sleep: int
        :param max_sleep: Max sleep time for retries in milliseconds
        :type max_sleep: int
        :param sock_connect_timeout: Socket connection timeout in seconds
        :type sock_connect_timeout: int
        :param sock_request_timeout: Request timeout in seconds
        :type sock_request_timeout: int
        :param use_datanode_hostname: Use hostname instead of IP address to commuicate with datanodes
        :type use_datanode_hostname: boolean
        '''
        self.use_trash = use_trash
        self.effective_user = effective_user
        self.use_sasl = use_sasl
        self.hdfs_namenode_principal = hdfs_namenode_principal
        self.max_failovers = max_failovers
        self.max_retries = max_retries
        self.base_sleep = base_sleep
        self.max_sleep = max_sleep
        self.sock_connect_timeout = sock_connect_timeout
        self.sock_request_timeout = sock_request_timeout
        self.use_datanode_hostname = use_datanode_hostname

        self.failovers = -1
        self.retries = -1

        if not namenodes:
            # Using InvalidInputException instead of OutOfNNException because the later is transient but current case
            # is not.
            raise InvalidInputException("List of namenodes is empty - couldn't create the client")
        self.namenode = self._switch_namenode(namenodes)
        self.namenode.next()

    def _check_failover(self, namenodes):
        if (self.failovers == -1):
            return
        elif (self.failovers >= self.max_failovers):
            msg = "Request tried and failed for all %d namenodes after %d failovers: " % (len(namenodes), self.failovers)
            for namenode in namenodes:
                msg += "\n\t* %s:%d" % (namenode.host, namenode.port)
            msg += "\nLook into debug messages - add -D flag!"
            log.debug(msg)
            raise OutOfNNException(msg)
        log.debug("Failover attempt %d:", self.failovers)
        self.__do_retry_sleep(self.failovers)
        self.failovers += 1

    def _switch_namenode(self, namenodes):
        while (True):
            for namenode in namenodes:
                self._check_failover(namenodes)
                log.debug("Switch to namenode: %s:%d" % (namenode.host, namenode.port))
                yield super(HAClient, self).__init__(namenode.host,
                                                     namenode.port,
                                                     namenode.version,
                                                     self.use_trash,
                                                     self.effective_user,
                                                     self.use_sasl,
                                                     self.hdfs_namenode_principal,
                                                     self.sock_connect_timeout,
                                                     self.sock_request_timeout,
                                                     self.use_datanode_hostname)


    def __calculate_exponential_time(self, time, retries, cap):
        # Same calculation as the original Hadoop client but converted to seconds
        baseTime = min(time * (1L << retries), cap);
        return (baseTime * (random.random() + 0.5)) / 1000;

    def __do_retry_sleep(self, retries):
        # Don't wait for the first retry.
        if (retries <= 0):
            sleep_time = 0
        else:
            sleep_time = self.__calculate_exponential_time(self.base_sleep, retries, self.max_sleep)
        log.debug("Doing retry sleep for %s seconds", sleep_time)
        time.sleep(sleep_time)

    def __should_retry(self):
        if self.retries >= self.max_retries:
            return False
        else:
            log.debug("Running retry %d of %d", self.retries, self.max_retries)
            self.__do_retry_sleep(self.retries)
            self.retries += 1
            return True

    def __handle_request_error(self, exception):
        log.debug("Request failed with %s" % exception)
        if exception.args[0].startswith("org.apache.hadoop.ipc.StandbyException"):
            self.namenode.next() # Failover and retry until self.max_failovers was reached
        elif exception.args[0].startswith("org.apache.hadoop.ipc.RetriableException") and self.__should_retry():
            return
        else:
            # There's a valid NN in active state, but there's still request error - raise
            # The Java Hadoop client does retry exceptions that are instance of IOException but
            # not instance of RemoteException here. However some cases have an at most once flag
            # thus we should not simply retry everything here. Let's fail it for now.
            raise

    def __handle_socket_error(self, exception):
        log.debug("Request failed with %s" % exception)
        if exception.errno in (errno.ECONNREFUSED, errno.EHOSTUNREACH):
            # if NN is down or machine is not available, pass it:
            self.namenode.next() # Failover and retry until self.max_failovers was reached
        elif isinstance(exception, socket.timeout):
            self.namenode.next() # Failover and retry until self.max_failovers was reached
        else:
            raise

    @staticmethod
    def _ha_return_method(func):
        ''' Method decorator for 'return type' methods '''
        def wrapped(self, *args, **kw):
            self._reset_retries()
            while(True): # switch between all namenodes
                try:
                    return func(self, *args, **kw)
                except RequestError as e:
                    self.__handle_request_error(e)
                except socket.error as e:
                    self.__handle_socket_error(e)
        return wrapped

    @staticmethod
    def _ha_gen_method(func):
        ''' Method decorator for 'generator type' methods '''
        def wrapped(self, *args, **kw):
            self._reset_retries()
            while(True): # switch between all namenodes
                try:
                    results = func(self, *args, **kw)
                    while(True): # yield all results
                        yield results.next()
                except RequestError as e:
                    self.__handle_request_error(e)
                except socket.error as e:
                    self.__handle_socket_error(e)
        return wrapped

HAClient._wrap_methods()

class AutoConfigClient(HAClient):
    ''' A pure python HDFS client that support HA and is auto configured through the ``HADOOP_HOME`` environment variable.

    HAClient is fully backwards compatible with the vanilla Client and can be used for a non HA cluster as well.
    This client tries to read ``${HADOOP_HOME}/conf/hdfs-site.xml`` and ``${HADOOP_HOME}/conf/core-site.xml``
    to get the address of the namenode.

    The behaviour is the same as Client.

    **Example:**

    >>> from snakebite.client import AutoConfigClient
    >>> client = AutoConfigClient()
    >>> for x in client.ls(['/']):
    ...     print x

    .. note::
        Different Hadoop distributions use different protocol versions. Snakebite defaults to 9, but this can be set by passing
        in the ``hadoop_version`` parameter to the constructor.
    '''
    def __init__(self, hadoop_version=Namenode.DEFAULT_VERSION, effective_user=None, use_sasl=False):
        '''
        :param hadoop_version: What hadoop protocol version should be used (default: 9)
        :type hadoop_version: int
        :param effective_user: Effective user for the HDFS operations (default: None - current user)
        :type effective_user: string
        :param use_sasl: Use SASL for authenication or not
        :type use_sasl: boolean
        '''

        configs = HDFSConfig.get_external_config()
        nns = [Namenode(nn['namenode'], nn['port'], hadoop_version) for nn in configs['namenodes']]
        if not nns:
            raise InvalidInputException("List of namenodes is empty - couldn't create the client")

        super(AutoConfigClient, self).__init__(nns, configs.get('use_trash', False), effective_user,
                                               configs.get('use_sasl', False), configs.get('hdfs_namenode_principal', None),
                                               configs.get('failover_max_attempts'), configs.get('client_retries'),
                                               configs.get('client_sleep_base_millis'), configs.get('client_sleep_max_millis'),
                                               10000, configs.get('socket_timeout_millis'),
                                               use_datanode_hostname=configs.get('use_datanode_hostname', False))
