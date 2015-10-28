"""
smartammDataGenerator.py
This python library contains the functions to generate fake Smart AMM data to test DaQ system.
Version 1.0
Date 12/01/2015
Developed by Sergi Rotger Griful <srgr@eng.au.dk>
SAMRE001{"Time":"2014-12-08T17:10:37.124Z","Device":"0015BC0026000015","registers":[{"value":171139380,"attribute":0,"type":"Wh_W","channel":100},{"value":140,"attribute":1024,"type":"Wh_W","channel":102}]}
"""

import datetime as dt
import json


class KaribuMessage(object):
    """Generic message that is send from Karibu to RabbitMQ."""

    def __str__(self):
        return self.type + self.payload


class SAMRE001(KaribuMessage):

    def __init__(self):
        self.type = 'SAMRE001'
        self.time = dt.datetime.now().isoformat()
        self.device = '0015BC0026000015'
        self.registers = []
        self.payload = json.dumps({"Time": self.time , "Device": self.device, "registers": self.registers})

    def add_register(self, value=171139380, attribute=0, reg_type='Wh_W', channel=100):
        register = {"value": value, "attribute": attribute, "type": reg_type, "channel": channel}
        print register
        self.registers.append(register)
        self.payload = json.dumps({"Time": self.time , "Device": self.device, "registers": self.registers})


if __name__ == "__main__":
    smart = SAMRE001()
    smart.add_register()
    smart.add_register()
    print smart