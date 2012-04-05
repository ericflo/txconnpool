##
# Copyright (c) 2010 Mochi Media, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##

from twisted.protocols.memcache import MemCacheProtocol

from txconnpool.pool import PooledClientFactory, Pool


class _PooledMemCacheProtocol(MemCacheProtocol):
    """
    A MemCacheProtocol that will notify a connectionPool that it is ready
    to accept requests.
    """
    factory = None

    def connectionMade(self):
        """
        Notify our factory that we're ready to accept connections.
        """
        MemCacheProtocol.connectionMade(self)

        self.factory.connectionPool.clientFree(self)

        if self.factory.deferred is not None:
            self.factory.deferred.callback(self)
            self.factory.deferred = None


class _MemCacheClientFactory(PooledClientFactory):
    """
    L{PooledClientFactory} that uses L{_PooledMemCacheProtocol}
    """
    protocol = _PooledMemCacheProtocol


class MemCachePool(Pool):
    """
    A MemCache client which is backed by a pool of connections.

    Usage Example::
        from twisted.internet.address import IPv4Address
        from txconnpool.memcache import MemCachePool

        addr = IPv4Address('TCP', '127.0.0.1', 11211)
        mc_pool = MemCachePool(addr, maxClients=20)

        d = mc_pool.get('cached-data')

        def gotCachedData(data):
            flags, value = data
            if value:
                print 'Yay, we got a cache hit'
            else:
                print 'Boo, it was a cache miss'

        d.addCallback(gotCachedData)
    """
    clientFactory = _MemCacheClientFactory

    def get(self, *args, **kwargs):
        """
        See L{twisted.protocols.memcache.MemCacheProtocol.get}.
        """
        return self.performRequest('get', *args, **kwargs)

    def set(self, *args, **kwargs):
        """
        See L{twisted.protocols.memcache.MemCacheProtocol.set}
        """
        return self.performRequest('set', *args, **kwargs)

    def delete(self, *args, **kwargs):
        """
        See L{twisted.protocols.memcache.MemCacheProtocol.delete}
        """
        return self.performRequest('delete', *args, **kwargs)

    def add(self, *args, **kwargs):
        """
        See L{twisted.protocols.memcache.MemCacheProtocol.add}
        """
        return self.performRequest('add', *args, **kwargs)
