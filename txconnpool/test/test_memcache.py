##
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

from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet.defer import Deferred

from txconnpool.test.test_basic import ADDRESS, StubReactor, StubConnectionPool

from txconnpool import memcache


class CallRecorder(object):
    """
    Simple call recorder for use with L{TestCase.patch}

    @ivar calls: List of tuples of args and kwargs.
    @type calls: C{[(tuple, dict)]}
    """
    def __init__(self):
        self.calls = []

    def call(self, *args, **kwargs):
        """
        Record all arguments and keyword arguments.
        """
        self.calls.append((args, kwargs))


class MemCachePoolTestCase(TestCase):
    def setUp(self):
        self.reactor = StubReactor()
        self.pool = memcache.MemCachePool(
          ADDRESS, maxClients=5, reactor=self.reactor)

        self.call_recorder = CallRecorder()
        self.patch(memcache.Pool,
                   'performRequest',
                   self.call_recorder.call)

    def test_get(self):
        """
        L{MemCachePool.get} properly calls L{Pool.performRequest}
        """
        self.pool.get('foo', 'bar', somekwarg=1)
        self.assertEqual(self.call_recorder.calls,
                         [(('get', 'foo', 'bar'), {'somekwarg': 1})])

    def test_set(self):
        """
        L{MemCachePool.set} properly calls L{Pool.performRequest}
        """
        self.pool.set('foo', 'bar', ttl=10000)
        self.assertEqual(self.call_recorder.calls,
                         [(('set', 'foo', 'bar'), {'ttl': 10000})])

    def test_delete(self):
        """
        L{MemCachePool.delete} properly calls L{Pool.performRequest}
        """
        self.pool.delete('foo', somekwarg=1)
        self.assertEqual(self.call_recorder.calls,
                         [(('delete', 'foo'), {'somekwarg': 1})])

    def test_add(self):
        """
        L{MemCachePool.add} properly calls L{Pool.performRequest}
        """
        self.pool.add('foo', 'bar', ttl=10000)
        self.assertEqual(self.call_recorder.calls,
                         [(('add', 'foo', 'bar'), {'ttl': 10000})])


class PooledMemCacheProtocolTestCase(TestCase):
    def setUp(self):
        self.protocol = memcache._PooledMemCacheProtocol()

        self.pool = StubConnectionPool()

        self.factory = memcache._MemCacheClientFactory()
        self.factory.connectionPool = self.pool

        self.factory.protocol = lambda: self.protocol
        self.factory.buildProtocol(ADDRESS)

    def test_connectionMadeCallsBase(self):
        """
        L{memcache._PooledMemCacheProtocol.connectionMade}
        calls L{MemCacheProtocol.connectionMade}
        """
        cm_cr = CallRecorder()
        self.patch(memcache.MemCacheProtocol, 'connectionMade', cm_cr.call)

        self.protocol.connectionMade()

        self.assertEqual(cm_cr.calls, [((self.protocol,), {})])

    def test_connectionMadeFreesClient(self):
        """
        L{memcache._PooledMemCacheProtocol.connectionMade} adds
        the current protocol instance to the pool and marks it as free.
        """
        self.protocol.connectionMade()
        self.assertEqual(self.pool.calls, [('free', self.protocol)])

    def test_connectionMadeFiresFactoryDeferred(self):
        """
        L{memcache._PooledMemCacheProtocol.connectionMade} fires the
        L{PooledClientFactory.deferred} and sets it as C{None}
        """
        d = self.factory.deferred

        def _checkDeferred(ign):
            self.assertIdentical(self.factory.deferred, None)

        def _checkProtocol(proto):
            self.assertEqual(proto, self.protocol)

            d2 = Deferred()
            d2.addCallback(_checkDeferred)

            reactor.callLater(0, d2.callback, None)

            return d2

        d.addCallback(_checkProtocol)

        self.protocol.connectionMade()

        return d

    def test_connectionMadeDoesntFireDeferredMultipleTimes(self):
        """
        L{memcache._PooledMemCacheProtocol.connectionMade} shouldn't fire the
        L{PooledClientFactory.deferred} if it is C{None}
        """
        d = self.factory.deferred

        def _checkDeferred(ign):
            self.assertIdentical(self.factory.deferred, None)

            # At this point factory.deferred is None so calling connectionMade
            # would raise an L{AttributeError} if connectionMade called back
            # the deferred.
            self.protocol.connectionMade()

        def _checkProtocol(proto):
            self.assertEqual(proto, self.protocol)

            d2 = Deferred()
            d2.addCallback(_checkDeferred)

            reactor.callLater(0, d2.callback, None)

            return d2

        d.addCallback(_checkProtocol)

        self.protocol.connectionMade()

        return d
