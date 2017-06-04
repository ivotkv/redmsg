# -*- coding: utf-8 -*-
# 
# The MIT License (MIT)
# 
# Copyright (c) 2017 Ivo Tzvetkov
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
# 

from redis import StrictRedis

__all__ = ['Subscriber']

class Subscriber(object):

    def __init__(self, redis=None, **redis_config):
        self.redis = redis if redis is not None else StrictRedis(**redis_config)
        self.pubsub = self.redis.pubsub()

    def subscribe(self, channel):
        return self.pubsub.subscribe('redmsg:' + channel)

    def unsubscribe(self, channel):
        return self.pubsub.unsubscribe('redmsg:' + channel)

    def listen(self):
        for msg in self.pubsub.listen():
            if msg['type'] == 'message':
                txid, data = msg['data'].decode('utf-8').split(':', 1)
                yield {
                    'channel': msg['channel'].decode('utf-8')[7:],
                    'txid': int(txid),
                    'data': data
                }

    def replay_from(self, channel, txid, batch_size=100):
        txid = int(txid)
        done = False
        while not done:
            for idx, data in enumerate(self.redis.mget(['redmsg:{0}:{1}'.format(channel, txid + i) for i in range(batch_size)])):
                if data is None:
                    done = True
                    break
                yield {
                    'channel': channel,
                    'txid': txid + idx,
                    'data': data.decode('utf-8')
                }
            txid += batch_size
