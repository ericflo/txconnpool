##
# Copyright (c) 2008 Apple Inc. All rights reserved.
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

from twisted.python.failure import Failure
from twisted.internet.defer import Deferred, fail
from twisted.internet.protocol import ReconnectingClientFactory


class NoSuchCommand(Exception):
    """
    Exception raised when a non-existent command is called.
    """



class PooledClientFactory(ReconnectingClientFactory):
    """
    A client factory for a protocol that reconnects and notifies a pool of it's
    state.

    @ivar connectionPool: A managing connection pool that we notify of events.
    @ivar deferred: A L{Deferred} that represents the initial connection.
    @ivar _protocolInstance: The current instance of our protocol that we pass
        to our connectionPool.
    """
    protocol = None
    connectionPool = None
    _protocolInstance = None


    def __init__(self):
        self.deferred = Deferred()


    def clientConnectionLost(self, connector, reason):
        """
        Notify the connectionPool that we've lost our connection.
        """

        if self.connectionPool.shutdown_requested:
            # The reactor is stopping; don't reconnect
            return

        if self._protocolInstance is not None:
            self.connectionPool.clientBusy(self._protocolInstance)

        ReconnectingClientFactory.clientConnectionLost(
            self,
            connector,
            reason)


    def clientConnectionFailed(self, connector, reason):
        """
        Notify the connectionPool that we're unable to connect
        """
        if self._protocolInstance is not None:
            self.connectionPool.clientBusy(self._protocolInstance)

        ReconnectingClientFactory.clientConnectionFailed(
            self,
            connector,
            reason)

    def buildProtocol(self, addr):
        """
        Attach the C{self.connectionPool} to the protocol so it can tell it,
        when we've connected.
        """
        if self._protocolInstance is not None:
            self.connectionPool.clientGone(self._protocolInstance)

        self._protocolInstance = self.protocol()
        self._protocolInstance.factory = self
        return self._protocolInstance



class Pool(object):
    """
    A connection pool.

    @ivar clientFactory: The L{ClientFactory} implementation that will be used
        for each protocol.

    @ivar _maxClients: A C{int} indicating the maximum number of clients.
    @ivar _serverAddress: An L{IAddress} provider indicating the server to
        connect to.  (Only L{IPv4Address} currently supported.)
    @ivar _reactor: The L{IReactorTCP} provider used to initiate new
        connections.

    @ivar _busyClients: A C{set} that contains all currently busy clients.
    @ivar _freeClients: A C{set} that contains all currently free clients.
    @ivar _pendingConnects: A C{int} indicating how many connections are in
        progress.
    """
    clientFactory = None # Should be set to the subclassed PooledClientFactory

    def __init__(self, serverAddress, maxClients=5, reactor=None):
        """
        @param serverAddress: An L{IPv4Address} indicating the server to
            connect to.
        @param maxClients: A C{int} indicating the maximum number of clients.
        @param reactor: An L{IReactorTCP{ provider used to initiate new
            connections.
        """
        self._serverAddress = serverAddress
        self._maxClients = maxClients

        if reactor is None:
            from twisted.internet import reactor
        self._reactor = reactor

        self.shutdown_deferred = None
        self.shutdown_requested = False
        reactor.addSystemEventTrigger('before', 'shutdown', self._shutdownCallback)

        self._busyClients = set([])
        self._freeClients = set([])
        self._pendingConnects = 0
        self._commands = []

    def _isIdle(self):
        return (
            len(self._busyClients) == 0 and
            len(self._commands) == 0 and
            self._pendingConnects == 0
        )

    def _shutdownCallback(self):
        self.shutdown_requested = True
        
        for client in self._busyClients:
            client.transport.loseConnection()
        for client in self._freeClients:
            client.transport.loseConnection()
        
        if self._isIdle():
            return None
        
        self.shutdown_deferred = Deferred()
        return self.shutdown_deferred

    def _newClientConnection(self):
        """
        Create a new client connection.

        @return: A L{Deferred} that fires with the L{IProtocol} instance.
        """
        self._pendingConnects += 1

        def _connected(client):
            self._pendingConnects -= 1

            return client

        factory = self.clientFactory()
        factory.noisy = False

        factory.connectionPool = self

        self._reactor.connectTCP(self._serverAddress.host,
                                 self._serverAddress.port,
                                 factory)
        d = factory.deferred

        d.addCallback(_connected)
        return d


    def _performRequestOnClient(self, client, method, *args, **kwargs):
        """
        Perform the given request on the given client.

        @param client: A L{Protocol} instance that will be used to perform
            the given request.

        @param method: The method to perform on the client.

        @parma args: Any positional arguments that should be passed to
            C{method}.
        @param kwargs: Any keyword arguments that should be passed to
            C{method}.

        @return: A L{Deferred} that fires with the result of the given command.
        """
        def _freeClientAfterRequest(result):
            self.clientFree(client)
            return result

        self.clientBusy(client)
        
        method = getattr(client, method, None)
        if method is not None:
            d = method(*args, **kwargs)
        else:
            d = fail(Failure(NoSuchCommand()))

        d.addCallback(_freeClientAfterRequest)

        return d


    def performRequest(self, method, *args, **kwargs):
        """
        Select an available client and perform the given request on it.

        @parma method: The method to call on the client.

        @parma args: Any positional arguments that should be passed to
            C{command}.
        @param kwargs: Any keyword arguments that should be passed to
            C{command}.

        @return: A L{Deferred} that fires with the result of the given command.
        """

        if len(self._freeClients) > 0:
            client = self._freeClients.pop()

            d = self._performRequestOnClient(
                client, method, *args, **kwargs)

        elif len(self._busyClients) + self._pendingConnects >= self._maxClients:
            d = Deferred()
            self._commands.append((d, method, args, kwargs))

        else:
            d = self._newClientConnection()
            d.addCallback(self._performRequestOnClient, method,
                *args, **kwargs)

        return d


    def clientGone(self, client):
        """
        Notify that the given client is to be removed from the pool completely.

        @param client: An instance of a L{Protocol}.
        """
        if client in self._busyClients:
            self._busyClients.remove(client)

        elif client in self._freeClients:
            self._freeClients.remove(client)


    def clientBusy(self, client):
        """
        Notify that the given client is being used to complete a request.

        @param client: An instance of C{self.clientFactory}
        """
        if client in self._freeClients:
            self._freeClients.remove(client)

        self._busyClients.add(client)


    def clientFree(self, client):
        """
        Notify that the given client is free to handle more requests.

        @param client: An instance of C{self.clientFactory}
        """
        if client in self._busyClients:
            self._busyClients.remove(client)

        self._freeClients.add(client)

        if self.shutdown_deferred and self._isIdle():
            self.shutdown_deferred.callback(None)

        if len(self._commands) > 0:
            d, method, args, kwargs = self._commands.pop(0)

            _ign_d = self.performRequest(method, *args, **kwargs)

            _ign_d.addCallback(d.callback)


    def suggestMaxClients(self, maxClients):
        """
        Suggest the maximum number of concurrently connected clients.

        @param maxClients: A C{int} indicating how many client connections we
            should keep open.
        """
        self._maxClients = maxClients
