from usepy_plugin_redis import useRedisStore, useRedisLock

rds = useRedisStore()
lock = useRedisLock(rds, timeout=10 * 1000)


def job():
    lock.acquire(blocking=True)
    print("Job started")


if __name__ == '__main__':
    for _ in range(10):
        job()
