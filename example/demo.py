from usepy_plugin_redis import useRedisStreamStore

r = useRedisStreamStore('mystream', 'mygroup')
# start_id, messages, deleted_msg_ids = r.connection.xautoclaim('mystream', 'mygroup', "consumer1", min_idle_time=0, count=2)
# pass
# [b'0-0', [], []]
# [b'1691925126600-0', [(b'1691925079930-0', {b'field1': b'value1', b'field2': b'value2'}), (b'1691925086315-0', {b'field1': b'value3', b'field2': b'value4'})], []]
messages = r.connection.xreadgroup('mygroup', "consumer1", {'mystream': '>'}, count=2)
pass
# []
# [(b'1691925126600-0', [(b'1691925079930-0', {b'field1': b'value1', b'field2': b'value2'}), (b'1691925086315-0', {b'field1': b'value3', b'field2': b'value4'})], [])]