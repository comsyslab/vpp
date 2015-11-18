# coding=utf-8

__author__ = 'srgr'

import pika
import datetime as dt
import random as rd
import time as t
import rabbitMQ as rmq
import sys

MAX_GFKSC002 = 3
T_GFKSC002 = 10
MAX_GFKRE003 = 25
T_GFKRE003 = 1

def fake_data_GFKRE003(n_sen=3180, n_ap=159):
    """
    This function generates fake GFKRE003 grundfos data.
    """

    t_now = dt.datetime.now().isoformat()
    base = 'GFKRE003{"timestamp":"%s","reading":[' %t_now

    sen_list = range(1, n_sen, 1)
    ap_list = range(1, n_ap, 1)
    value_list = range(1, n_sen*20, 1)

    for i in range(1,200,1):
        sen_id = rd.choice(sen_list)
        ap_id = rd.choice(ap_list)
        value = float(rd.choice(value_list))
        t_now = dt.datetime.now().isoformat()
        base_sen = '{"sensorId":%s,"appartmentId":%s,"value":%s,' \
          '"timestamp":"%s"}' %(sen_id, ap_id, value, t_now)

        base += base_sen
    base = base.replace('}{', '},{')
    return base+'],"version":3}'


def fake_data_GFKSC002(n_sen = 3180, n_ap = 159):
    """
    This function generates fake GFKSC002 grundfos data.
    """
    description_list = ['Accumulated Energy consumption kWh','Ambient Temperature °C', 'CO2 Level',
                        'Relative Humidity %', 'Real Cold Flow GF sensor l/min', 'Accumulated cold water m3',
                        'Real Hot Flow GF sensor l/min','Real Hot water temperature GF sensor °C',
                        'Accumulated Hot water m3', 'Instant  Heat Energy consumption W',
                        'Accumulated Heat Energy consumption Wh', 'Flow l/min', 'Accumulated water m3',
                        'Inlet Temperature °C', 'Outlet Temperature °C', 'Instant  Heat Energy consumption W',
                        'Accumulated Heat Energy consumption Wh', 'Accumulated cold water m3',
                        'Accumulated Hot water m3']
    units_list = ['kWh', '°C', 'ppm', '%', 'l/min', 'm3', 'l/min', '°C', 'm3', 'W', 'Wh', 'l/min', 'm3', '°C', '°C',
                  'W', 'Wh', 'm3', 'm3']

    floor_list = range(1, 11, 1)
    door_list = range(1, 9, 1)
    size_list = [16.0, 20.3, 25.0]

    t_now = dt.datetime.now().isoformat()
    base = 'GFKSC002{"timestamp":"%s","sensorCharacteristic":[' % t_now

    sen_list = range(1, n_sen, 1)
    ap_list = range(1, n_ap, 1)

    for sen_id in sen_list:
        description = rd.choice(description_list)
        units = rd.choice(units_list)

        base_sen = '{"sensorId":%s,"description":"%s","unit":"%s","externalRef":"[fk_002];[fk_004]",' \
                    '"calibrationEquation":"A+B*x+C*[fk_002]+ [fk_004]","calibrationCoeff":"24.2;33.2;32.87",' \
                    '"calibrationDate":"2013-12-17T14:54:01Z"}' %(sen_id, description, units)

        base += base_sen

    base += '],"appartmentCharacteristic":['

    for ap_id in ap_list:
        floor = rd.choice(floor_list)
        door = rd.choice(door_list)
        size = rd.choice(size_list)

        base_ap = '{"appartmentId":%s,"floor":%s,"no":%s,"size":%s}' % (ap_id, floor, door, size)

        base += base_ap

    base = base.replace('}{', '},{')

    return base+'],"version":2}'


def write_data_files():
    f1 = open('GFKSC002.txt', 'w')
    f2 = open('GFKRE003.txt', 'w')
    m1 = fake_data_GFKSC002()
    m2 = fake_data_GFKRE003()
    f1.write(m1)
    f2.write(m2)
    f1.close()
    f2.close()
    sys.exit()


exchange = rmq.Exchange('./configRabbitMQ.ini')

# Connection parameters: option 1

credentials = pika.PlainCredentials(exchange.username, exchange.password)
connection = pika.BlockingConnection(pika.ConnectionParameters(
    host=exchange.url, credentials=credentials, ssl=exchange.ssl, port=exchange.port))
channel = connection.channel()

# Connection parameters: option 2 -- currently (08/05/2014) not working
"""
aux_url = 'amqps://' + exchange.username + ':' + exchange.password + '@' + exchange.url + ':' + str(
    exchange.port) + '/%2f'
connection = pika.BlockingConnection(url_parameters)
channel = connection.channel()
"""

channel.exchange_declare(exchange=exchange.name, type=exchange.type, durable=exchange.durable)

# Here you can modify the data messages that you want to send
routing_key = 'grundfos.sensors'

if sys.argv[-1] == 'sc':
    message = fake_data_GFKSC002()
    for i in range(1, MAX_GFKSC002):
        print type(message)
        channel.basic_publish(exchange='', routing_key=routing_key, body=message)
        print type(message)
        print " [x] Sent %r:%r" % (routing_key, message[:8])
        message = fake_data_GFKSC002()
        t.sleep(T_GFKSC002)
elif sys.argv[-1] == 're':
    message = fake_data_GFKRE003()
    for i in range(1, MAX_GFKRE003):
        channel.basic_publish(exchange='', routing_key=routing_key, body=message)
        print " [x] Sent %r:%r" % (routing_key, message[:8])
        message = fake_data_GFKRE003()
        t.sleep(T_GFKRE003)
else:
    print("Append 'sc' or 're' to send GFKSC002 and GFKRE003 messages respectively.")


connection.close()
