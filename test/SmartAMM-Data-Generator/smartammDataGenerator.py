# coding=utf-8

__author__ = 'srgr'

import pika
import datetime as dt
import random as rd
import time as t
import rabbitMQ as rmq
import sys
import json
import os

def get_smartamm_messages():


    messages = []

    dir_name = 'develco_data_samples'
    for file_name in os.listdir(dir_name):
        file = open(dir_name + os.sep + file_name, 'r')
        messages.append(file.read())
        file.close()

    return messages

def send():
    # Connection parameters
    exchange = rmq.Exchange('./configRabbitMQ.ini')
    credentials = pika.PlainCredentials(exchange.username, exchange.password)
    connection = pika.BlockingConnection(
                    pika.ConnectionParameters(host=exchange.url,
                                              credentials=credentials,
                                              ssl=exchange.ssl,
                                              port=exchange.port))
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange.name, type=exchange.type, durable=exchange.durable)

    # Here you can modify the data messages that you want to send
    routing_key = 'smartamm.data'

    messages = get_smartamm_messages()


    interval = 0.1 #seconds

    for i in range(0, 50):
        for message in messages:
            channel.basic_publish(exchange='', routing_key=routing_key, body=message)
            print " [x] Sent %r:%r" % (routing_key, message[:50])
            t.sleep(interval)

    connection.close()




if __name__ == '__main__':
    send()