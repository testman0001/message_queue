#!/usr/bin/env python3
# -*- coding:UTF-8 -*-

import pika
import time


class Producer(object):
    def __init__(self) -> None:
        _credentials = pika.PlainCredentials('dev', 'KedaRmq!Dev_56')
        _parameters = pika.ConnectionParameters(host='10.67.18.194',
                                                port=5682,
                                                credentials=_credentials)
        self._conn = pika.BlockingConnection(_parameters)
        self._chan = self._conn.channel()
        #self._chan.queue_declare('cmdataproxy.conf.q:10.67.18.197:5')
        print('init finished.')

    def send_message_with_big_header(self):
        '''
        header塞入过大内容不会自动切分，导致帧过大断开连接
        error:frame_too_large
        '''
        headers = {'info': '.' * 200000}
        prop = pika.BasicProperties(headers=headers)
        body = ''
        self._chan.basic_publish(exchange='', routing_key='test',
                                 properties=prop,
                                 body=body)

    def send_message_with_big_body(self):
        '''
        body能自动切分成多个frame，不会有问题
        '''
        body = '.' * 2000000
        self._chan.basic_publish(exchange='', routing_key='test',
                                 body=body)

    def send_messages(self, num):
        for i in range(num):
            print(i)
            self._chan.basic_publish(exchange='dssmaster.rcv.ex', routing_key='682ad804-76cf-11ee-8b02-a0369f289c50',
                                     body='.')

    def run(self):
        pass


if __name__ == '__main__':
    p = Producer()
    # p.send_message_with_big_header()
    # p.send_message_with_big_body()
    p.send_messages(100)
    time.sleep(300)
