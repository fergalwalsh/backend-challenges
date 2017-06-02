import time
from client import Client


class FWDBClient(Client):

    def ping(self):
        return self.execute('PING')

    def get(self, key):
        return self.execute('GET', key=key)

    def set(self, key, value, ttl=None):
        return self.execute('SET', key=key, value=value, ttl=ttl)

    def incr(self, key):
        return self.execute('INCR', key=key)

    def decr(self, key):
        return self.execute('DECR', key=key)

    def delete(self, key):
        return self.execute('DELETE', key=key)

    def expire(self, key, ttl):
        return self.execute('EXPIRE', key=key, ttl=ttl)

    def ttl(self, key):
        return self.execute('TTL', key=key)


if __name__ == '__main__':
    client = FWDBClient()

    while True:
        print(client.ping())
        time.sleep(0.2)
    # print(client.set('foo', 123))
    # print(client.ttl('foo'))
    # print(client.set('foo', 123, 5))
    # print(client.ttl('foo'))
    # print(client.expire('foo', 3))
    # print(client.ttl('foo'))
    # print(client.get('foo'))
    # print(client.incr('foo'))
    # print(client.incr('bar'))
    # print(client.incr('bar'))

    # print(client.decr('bar'))
    # print(client.decr('xyz'))

    # print(client.set('baz', 'abc'))
    # try:
    #     print(client.incr('baz'))
    # except Exception as e:
    #     print(e)

    # print(client.delete('baz'))
    # print(client.get('baz'))
    # print(client.delete('doesnotexist'))
    # print(client.ttl('bar'))
    # print(client.ttl('foo'))
    # for x in range(10000):
    #     client.set('ex%s' % x, x, 10)
    # print(client.get('foo'))
