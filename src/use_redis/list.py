from .store import RedisStore


class RedisListStore(RedisStore):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self._key = key

    @property
    def key(self):
        return self._key

    def lpush(self, *values):
        """将一个或多个值插入到列表头部"""
        return self.connection.lpush(self.key, *values)

    def rpush(self, *values):
        """将一个或多个值插入到列表尾部"""
        return self.connection.rpush(self.key, *values)

    def lpop(self):
        """移除并返回列表的第一个元素"""
        return self.connection.lpop(self.key)

    def rpop(self):
        """移除并返回列表的最后一个元素"""
        return self.connection.rpop(self.key)

    def length(self):
        """返回列表的长度"""
        return self.connection.llen(self.key)

    def range(self, start, end):
        """获取列表指定范围内的元素"""
        return self.connection.lrange(self.key, start, end)

    def set(self, index, value):
        """通过索引设置列表元素的值"""
        return self.connection.lset(self.key, index, value)

    def index(self, index):
        """通过索引获取列表中的元素"""
        return self.connection.lindex(self.key, index)

    def trim(self, start, end):
        """修剪列表，只保留指定区间内的元素"""
        return self.connection.ltrim(self.key, start, end)

    def remove(self, value, count=0):
        """移除列表中与value相等的元素"""
        return self.connection.lrem(self.key, count, value)

    def insert_before(self, pivot, value):
        """在列表的元素前插入元素"""
        return self.connection.linsert(self.key, 'BEFORE', pivot, value)

    def insert_after(self, pivot, value):
        """在列表的元素后插入元素"""
        return self.connection.linsert(self.key, 'AFTER', pivot, value)

    def __getattr__(self, name):
        """动态处理未实现的方法"""

        def method(*args, **kwargs):
            redis_method = getattr(self.connection, name)
            return redis_method(self.key, *args, **kwargs)

        return method
