from setuptools import setup, find_packages

version = '0.1.1'

LONG_DESCRIPTION = '''
txconnpool
==========

A generalized connection pooling library for Twisted.


Example Description
-------------------

Assume that we've got a web application, which performs some expensive 
computations, and then caches them in a memcached_ server.  The simple way to
achieve this in Twisted is to create a ClientCreator_ for the MemCacheProtocol_
and whenever we need to communicate with the server, we can simply use that.

This works for low volumes of queries, but let's say that now we start hitting 
memcached a lot--several times per web request, of which we are receiving many
per second.  Very quickly, the connection overhead can become a problem.

Instead of creating a new connection for every query, it would be much better 
to maintain a pool of open connections, and simply reuse those open 
connections; queuing up any queries if all of the connections are in use.  With
txconnpool, setting this up can be quite easy.


Example Implementation
----------------------

First we need to create a few classes of boilerplate, to transform a
MemCacheProtocol_ into a PooledMemcachedProtocol, and then create a pool::


    from twisted.protocols.memcache import MemCacheProtocol

    from txconnpool.pool import PooledClientFactory, Pool

    class PooledMemCacheProtocol(MemCacheProtocol):
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

    class MemCacheClientFactory(PooledClientFactory):
        protocol = PooledMemCacheProtocol

    class MemCachePool(Pool):
        clientFactory = MemCacheClientFactory
    
        def get(self, *args, **kwargs):
            return self.performRequest('get', *args, **kwargs)

        def set(self, *args, **kwargs):
            return self.performRequest('set', *args, **kwargs)

        def delete(self, *args, **kwargs):
            return self.performRequest('delete', *args, **kwargs)

        def add(self, *args, **kwargs):
            return self.performRequest('add', *args, **kwargs)


Now, with this having been created, we can go ahead and use it::


    from twisted.internet.address import IPv4Address
    
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


.. _memcached: http://memcached.org/
.. _ClientCreator: http://twistedmatrix.com/documents/current/api/twisted.internet.protocol.ClientCreator.html
.. _MemCacheProtocol: http://twistedmatrix.com/documents/current/api/twisted.protocols.memcache.MemCacheProtocol.html
'''

setup(
    name='txconnpool',
    version=version,
    description="A generalized connection pooling library for Twisted",
    long_description=LONG_DESCRIPTION,
    classifiers=[
        "Programming Language :: Python",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Twisted",
        "Environment :: Web Environment",
    ],
    keywords='twisted,connection,pool,connpool,txconnpool',
    author='Eric Florenzano',
    author_email='floguy@gmail.com',
    url='http://github.com/ericflo/txconnpool/',
    license='Apache',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
)