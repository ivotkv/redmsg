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
from threading import Thread, Event
try:
    from queue import Queue, Empty
except ImportError:
    from Queue import Queue, Empty
from .exc import *

__all__ = ['Subscriber']

LOOP_TIMEOUT = 0.01

class ThreadEvents(object):

    def __init__(self):
        self.terminate = Event()
        self.terminated = Event()
        self.exception = None

class Subscriber(object):

    def __init__(self, redis=None, **redis_config):
        self.redis = redis if redis is not None else StrictRedis(**redis_config)
        self.pubsub = self.redis.pubsub(ignore_subscribe_messages=True)
        self.channel = None

    def subscribe(self, channel):
        if self.channel is not None:
            raise ChannelError('already subscribed to a channel')
        self.pubsub.subscribe('redmsg:' + channel)
        self.channel = channel

    def unsubscribe(self, channel):
        if self.channel is None:
            raise ChannelError('not subscribed to a channel')
        self.pubsub.unsubscribe('redmsg:' + channel)
        self.channel = None

    def process_message(self, message):
        txid, data = message['data'].decode('utf-8').split(':', 1)
        return {
            'channel': message['channel'].decode('utf-8')[7:],
            'txid': int(txid),
            'data': data
        }

    def listen(self):
        if self.channel is None:
            raise ChannelError('not subscribed to a channel')
        for message in self.pubsub.listen():
            yield self.process_message(message)

    def _listener_thread(self, queue, events):
        try:
            while not events.terminate.is_set():
                message = self.pubsub.get_message(timeout=LOOP_TIMEOUT)
                if message is not None:
                    queue.put(self.process_message(message))
        except Exception as e:
            events.exception = e
        finally:
            events.terminated.set()

    def _loader_thread(self, queue, events, txid, batch_size, ignore_missing=False):
        try:
            listener_queue = Queue()
            listener_events = ThreadEvents()
            listener_thread = Thread(target=self._listener_thread, args=(listener_queue, listener_events))
            listener_thread.daemon = True
            listener_thread.start()

            latest = -1
            current = txid
            loaded = batch_size
            while (loaded == batch_size or (ignore_missing and loaded > 0)) and not events.terminate.is_set():
                loaded = 0
                keys = ['redmsg:{0}:{1}'.format(self.channel, current + i) for i in range(batch_size)]
                for idx, data in enumerate(self.redis.mget(keys)):
                    if data is None:
                        if ignore_missing:
                            continue
                        else:
                            break
                    else:
                        latest = current + idx
                        queue.put({
                            'channel': self.channel,
                            'txid': latest,
                            'data': data.decode('utf-8')
                        })
                        loaded += 1
                current += batch_size

            if not ignore_missing and latest == -1 and not events.terminate.is_set():
                raise MissingTransaction('txid not found: {0}'.format(txid))

            while not events.terminate.is_set() and not listener_events.terminated.is_set():
                try:
                    message = listener_queue.get(timeout=LOOP_TIMEOUT)
                    if message['txid'] > latest:
                        if not ignore_missing:
                            if message['txid'] != (latest + 1):
                                raise MissingTransaction('missing txid: {0}'.format(latest + 1))
                            else:
                                latest = message['txid']
                        queue.put(message)
                except Empty:
                    pass

        except Exception as e:
            events.exception = e

        finally:
            if listener_thread.is_alive():
                listener_events.terminate.set()
                listener_events.terminated.wait()
            if listener_events.exception:
                events.exception = listener_events.exception
            events.terminated.set()

    def listen_from(self, txid, batch_size=100, ignore_missing=False):
        if self.channel is None:
            raise ChannelError('not subscribed to a channel')
        txid = int(txid)

        loader_queue = Queue()
        loader_events = ThreadEvents()
        loader_thread = Thread(target=self._loader_thread, args=(loader_queue, loader_events, txid, batch_size, ignore_missing))
        loader_thread.daemon = True
        loader_thread.start()

        try:
            while not loader_events.terminated.is_set():
                try:
                    yield loader_queue.get(timeout=LOOP_TIMEOUT)
                except Empty:
                    pass
        finally:
            if loader_thread.is_alive():
                loader_events.terminate.set()
                loader_events.terminated.wait()
            if loader_events.exception:
                raise loader_events.exception
