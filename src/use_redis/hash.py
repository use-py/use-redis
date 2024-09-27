from .store import RedisStore


class RedisHashStore(RedisStore):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self._key = key

    @property
    def key(self):
        return self._key

    def set(self, field, value):
        """设置哈希表中指定字段的值"""
        return self.connection.hset(self.key, field, value)

    def set_multiple(self, mapping):
        """同时将多个 field-value 对设置到哈希表中"""
        return self.connection.hmset(self.key, mapping)

    def get(self, *fields):
        """获取哈希表中指定字段的值"""
        if len(fields) == 1:
            return self.connection.hget(self.key, fields[0])
        return self.connection.hmget(self.key, fields)

    def delete(self, *fields):
        """删除哈希表中的一个或多个字段"""
        return self.connection.hdel(self.key, *fields)

    def exists(self, field):
        """检查哈希表中是否存在指定的字段"""
        return self.connection.hexists(self.key, field)

    def length(self):
        """获取哈希表中字段的数量"""
        return self.connection.hlen(self.key)

    def keys(self):
        """获取哈希表中的所有字段"""
        return self.connection.hkeys(self.key)

    def values(self):
        """获取哈希表中所有字段的值"""
        return self.connection.hvals(self.key)

    def items(self):
        """获取哈希表中所有的字段和值"""
        return self.connection.hgetall(self.key)

    def increment(self, field, amount=1):
        """
        将哈希表中指定字段的值增加给定的增量
        
        :param field: 要增加的字段
        :param amount: 增加的数量，可以是整数或浮点数
        :return: 增加后的值
        """
        if isinstance(amount, int):
            return self.connection.hincrby(self.key, field, amount)
        else:
            return self.connection.hincrbyfloat(self.key, field, amount)


    def scan(self, cursor=0, match=None, count=None):
        """迭代哈希表中的键值对"""
        return self.connection.hscan(self.key, cursor, match, count)

    def __getattr__(self, name):
        """动态处理未实现的方法"""
        def method(*args, **kwargs):
            redis_method = getattr(self.connection, name)
            return redis_method(self.key, *args, **kwargs)
        return method
