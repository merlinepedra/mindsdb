import os
import time
import shelve
import json
from abc import ABC, abstractmethod
from pathlib import Path

import dill
import walrus

from mindsdb.utilities.config import Config

CONFIG = Config()


class BaseCache(ABC):
    def __init__(self):
        self.config = Config()
        self.max_size = self.config["cache"].get("max_size", 50)
        self.serializer = self.config["cache"].get('serializer', dill)

    def serialize(self, value):
        return self.serializer.dumps(value)

    def deserialize(self, value):
        return self.serializer.loads(value)

    # @abstractmethod
    # def delete(self):
    #     pass

    # @abstractmethod
    # def __getitem__(self, key):
    #     pass
    #
    # @abstractmethod
    # def __setitem__(self, key, value):
    #     pass


class FileCache(BaseCache):
    def __init__(self, category, path=None):
        super().__init__()

        if path is None:
            path = self.config['paths']['cache']

        # include category
        cache_path = Path(path) / category
        if not os.path.exists(cache_path):
            os.makedirs(cache_path)

        self.path = cache_path

    def clear_old_cache(self):
        # buffer to delete, to not run delete on every adding
        buffer_size = 5

        if self.max_size is None:
            return

        cur_count = len(os.listdir(self.path))

        # remove oldest
        if cur_count > self.max_size + buffer_size:

            files = sorted(Path(self.path).iterdir(), key=os.path.getmtime)
            for file in files[cur_count-self.max_size:]:
                self.delete_file(file)

    def file_path(self, name):
        return self.path / name

    def set(self, name, value):
        path = self.file_path(name)
        value = self.serialize(value)

        with open(path, 'wb') as fd:
            fd.write(value)
        self.clear_old_cache()

    def get(self, name):
        path = self.file_path(name)

        with open(path, 'rb') as fd:
            value = fd.read()
        value = self.deserialize(value)
        return value

    def delete(self, name):
        path = self.file_path(name)
        self.delete_file(path)

    def delete_file(self, path):
        os.unlink(path)


class RedisCache(BaseCache):
    def __init__(self, category, connection_info=None):
        super().__init__()

        self.category = category

        if connection_info is None:
            connection_info = self.config["cache"]["params"]
        self.client = walrus.Database(**connection_info)

    def clear_old_cache(self, key_added):

        if self.max_size is None:
            return

        # buffer to delete, to not run delete on every adding
        buffer_size = 5

        # using key with category name to store all keys with modify time
        self.client.hset(self.category, key_added, int(time.time()))

        cur_count = self.client.hlen(self.category)

        # remove oldest
        if cur_count > self.max_size + buffer_size:
            # 5 is buffer to delete, to not run delete on every adding

            keys = self.client.hgetall(self.category)
            # sort by timestamp
            keys.sort(key=lambda x: x[1])

            for key, _ in keys[cur_count - self.max_size]:
                self.delete_key(key)
                self.client.hset(self.category, key)

    def redis_key(self, name):
        return f'{self.category}_{name}'

    def set(self, name, value):
        key = self.redis_key(name)
        value = self.serialize(value)

        self.client.set(key, value)

        self.clear_old_cache(key)

    def get(self, name):
        key = self.redis_key(name)
        value = self.client.get(key)
        return self.deserialize(value)

    def delete(self, name):
        key = self.redis_key(name)

        self.delete_key(key)

    def delete_key(self, key):
        self.client.delete(key)


class LocalCache_not_used(BaseCache):
    def __init__(self, name, *args, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        self.cache_file = os.path.join(self.config['paths']['cache'], name)
        self.cache = shelve.open(self.cache_file, **kwargs)

    def __getattr__(self, name):
        return getattr(self.cache, name)

    def __getitem__(self, key):
        return self.cache.__getitem__(key)

    def __setitem__(self, key, value):
        return self.cache.__setitem__(key, value)

    def __enter__(self):
        if self.cache is None:
            self.cache = shelve.open(self.cache_file, **self.kwargs)
        return self.cache.__enter__()

    def __exit__(self, _type, value, traceback):
        if self.cache is None:
            return None
        res = self.cache.__exit__(_type, value, traceback)
        self.cache = None
        return res

    def __contains__(self, key):
        return key in self.cache

    def delete(self):
        try:
            self.cache.close()
        except Exception:
            pass
        os.remove(self.cache_file)


class RedisCache(BaseCache):
    def __init__(self, prefix, *args, **kwargs):
        super().__init__()
        self.prefix = prefix
        if self.config["cache"]["type"] != "redis":
            raise Exception(f"wrong cache type in config. expected 'redis', but got {self.config['cache']['type']}.")
        connection_info = self.config["cache"]["params"]
        self.client = walrus.Database(**connection_info)

    def __decode(self, data):

        if isinstance(data, dict):
            return dict((self.__decode(x), self.__decode(data[x])) for x in data)
        if isinstance(data, list):
            return list(self.__decode(x) for x in data)
        # assume it is string
        return data.decode("utf8")

    def __contains__(self, key):
        key = f"{self.prefix}_{key}"
        return key in self.__decode(self.client.keys())

    def __getitem__(self, key):
        key = f"{self.prefix}_{key}"
        raw = self.client.get(key)
        if raw is None:
            raise KeyError(key)
        try:
            res = json.loads(raw)
        except json.JSONDecodeError:
            res = raw.decode('utf8')
        return res

    def __setitem__(self, key, value):
        key = f"{self.prefix}_{key}"
        self.client.set(key, json.dumps(value))

    def __iter__(self):
        return iter(self.__decode(self.client.keys()))

    def __next__(self):
        for i in self.__decode(self.client.keys()):
            yield i

    def __delitem__(self, key):
        key = f"{self.prefix}_key"
        self.client.delete(key)

    def delete(self):
        pass


# Cache = RedisCache if CONFIG['cache']['type'] == 'redis' else LocalCache

def get_cache(category):
    config = Config()
    if config['cache']['type'] == 'redis':
        return RedisCache(category)
    else:
        return # don't use filecache yet
        return FileCache(category)
