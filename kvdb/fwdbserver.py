import time
import random
from dbserver import Server


class FWDBServer(Server):

    def __init__(self, **kwargs):
        self.db = {}
        self.ttls = {}
        self.access_time = {}
        self.max_keys = 100
        self.i = 0
        Server.__init__(self, **kwargs)

    def handle_message(self, message):
        self.i += 1
        if self.i == 10:
            self.purge_expired_keys()
            self.purge_lru()
            self.i = 0

        try:
            cmd = message['command'].lower()
            f = getattr(self, cmd)
            kwargs = message['args']
            try:
                result = f(**kwargs)
                response = {'status': 'OK', 'result': result}
            except Exception as e:
                response = {
                    'status': 'ERROR',
                    'message': e.message
                }
            print('%s keys' % len(self.db))
            return response
        except AttributeError:
            return {'message': 'Unknown command', 'status': 'ERROR'}

    def ping(self):
        return 'PONG'

    def set(self, key, value, ttl=None):
        self.db[key] = value
        self.access_time[key] = time.time()
        self.expire(key, ttl)

    def get(self, key):
        value = self._get(key)
        return value

    def incr(self, key):
        value = self._get(key, '0')
        try:
            value = int(value)
        except ValueError:
            raise Exception("Value is not an integer")
        self.db[key] = str(value + 1)
        return self.db[key]

    def decr(self, key):
        value = self._get(key, '0')
        try:
            value = int(value)
        except ValueError:
            raise Exception("Value is not an integer")
        self.db[key] = str(value - 1)
        return self.db[key]

    def delete(self, key):
        self.db.pop(key, None)
        self.ttls.pop(key, None)
        self.access_time.pop(key, None)

    def expire(self, key, ttl):
        if ttl is None:
            self.ttls.pop(key, None)
        else:
            self.ttls[key] = int(time.time()) + ttl

    def ttl(self, key):
        if key in self.ttls:
            ttl = int(self.ttls[key] - time.time())
        else:
            ttl = None
        return ttl

    def _get(self, key, default=None):
        if self._is_expired(key):
            self.delete(key)
            value = default
        else:
            try:
                value = self.db[key]
                self.access_time[key] = time.time()
            except KeyError:
                value = default
        return value

    def _is_expired(self, key):
        ttl = self.ttls.get(key, None)
        if not ttl:
            return False
        if time.time() < ttl:
            return False
        return True

    def purge_expired_keys(self):
        if len(self.ttls) > 10:
            keys = list(self.ttls.keys())
            sample = random.sample(keys, 10)
            for k in sample:
                if self._is_expired(k):
                    self.delete(k)

    def purge_lru(self):
        if len(self.db) > self.max_keys:
            sorted_volatile_keys = sorted(self.ttls, key=lambda k: self.access_time[k])
            print('%s volatile keys' % len(sorted_volatile_keys))
            for k in sorted_volatile_keys[:10]:
                self.delete(k)

if __name__ == '__main__':
    server = FWDBServer()
    server.run()
