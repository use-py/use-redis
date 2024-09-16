from .store import RedisStore


class RedisSetStore(RedisStore):
    def __init__(self, key, **kwargs):
        super().__init__(**kwargs)
        self._key = key

    @property
    def key(self):
        return self._key

    def add(self, *values):
        """向集合中添加一个或多个成员"""
        return self.connection.sadd(self.key, *values)

    def remove(self, *values):
        """从集合中移除一个或多个成员"""
        return self.connection.srem(self.key, *values)

    def members(self):
        """返回集合中的所有成员"""
        return self.connection.smembers(self.key)

    def is_member(self, value):
        """判断value是否是集合的成员"""
        return self.connection.sismember(self.key, value)

    def are_members(self, *values):
        """判断多个value是否是集合的成员"""
        return self.connection.sismember(self.key, *values)

    def size(self):
        """返回集合的成员数"""
        return self.connection.scard(self.key)

    def pop(self):
        """随机移除并返回集合中的一个成员"""
        return self.connection.spop(self.key)

    def move(self, dst_key, value):
        """将成员从当前集合移动到另一个集合"""
        return self.connection.smove(self.key, dst_key, value)

    def intersection(self, *other_keys):
        """返回当前集合与其他集合的交集"""
        return self.connection.sinter(self.key, *other_keys)

    def union(self, *other_keys):
        """返回当前集合与其他集合的并集"""
        return self.connection.sunion(self.key, *other_keys)

    def difference(self, *other_keys):
        """返回当前集合与其他集合的差集"""
        return self.connection.sdiff(self.key, *other_keys)

    def random_member(self):
        """随机返回集合中的一个成员，但不删除"""
        return self.connection.srandmember(self.key)

    def scan(self, cursor=0, match=None, count=None):
        """迭代集合中的元素"""
        return self.connection.sscan(self.key, cursor, match, count)

    def __getattr__(self, name):
        """动态处理未实现的方法"""

        def method(*args, **kwargs):
            redis_method = getattr(self.connection, name)
            return redis_method(self.key, *args, **kwargs)

        return method
