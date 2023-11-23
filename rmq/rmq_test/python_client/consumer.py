#!/usr/bin/env python3
# -*- coding:UTF-8 -*-

import pika
import time


class Consumer(object):
    def __init__(self) -> None:
        _credentials = pika.PlainCredentials('dev', 'KedaRmq!Dev_56')
        _parameters = pika.ConnectionParameters(host='172.16.237.6',
                                                port=6672,
                                                credentials=_credentials,
                                                heartbeat=6,
                                                )
        _parameters_long = pika.ConnectionParameters(host='172.16.237.6',
                                        port=6672,
                                        credentials=_credentials,
                                        heartbeat=6000)
        self._conn = pika.BlockingConnection(_parameters)
        self._chan = self._conn.channel()
        c = []
        ca = []
        for i in range(100):
            c.append(pika.BlockingConnection(_parameters_long))
            ca.append(c[i].channel())
        print('init finished.')

    def consume_message(self):
        # i = 0
        # while True:
        #     message = self._chan.basic_get('cmdataproxy.conf.q:10.67.18.197:5')
        #     print(message)
        #     self._chan.basic_ack(delivery_tag=message[0].delivery_tag)
        #     print(i)
        #     i += 1
        #     time.sleep(20)
        def callback(ch, method, properties, body):
            print("Received:", body)
            self._chan.basic_ack(delivery_tag=method.delivery_tag)
        # 监听队列,指定回调函数  
        
        self._chan.basic_qos(prefetch_count=100)
        self._chan.basic_consume(
            queue='cmdataproxy.conf.q.test', on_message_callback=callback)
        # time.sleep(60)
        # queue_obj = self._chan.queue_declare(queue='rms.notify.q.mytest', durable=True)
        # print("当前订阅数目"+str(queue_obj.method.consumer_count))
        self._chan.start_consuming()


if __name__ == '__main__':
    c = Consumer()
    c.consume_message()
    time.sleep(3)