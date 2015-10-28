# Grundfos Dormitory Lab (GDL) Data Generator

*Version 1 developed by Sergi Rotger Griful <srgr@eng.au.dk> on the 08/05/2014*


## Content
In this repository you can find the following documents:

* gdlDataGenerator.py: Python code able to generate data string emulating the Grundfos Dormitory Lab data 'GFKRE003'
and 'GFKSC002'
* rabbiMQ.py: Python library for connecting to a RabbitMQ server
* configRabbitMQ.ini: Configuration file with the connection parameters of the RabbitMQ server
* stable-re.txt: File with the Python packages required

## Requirements

* Python 2.7
* Python packages: pika 0.9.13
* RabbitMQ server running

## How to run it:

1. Install the required Python packages

2. Modify in gdlDataGenerator.py the last lines to set the data you want to send

3. Run gdlDataGenerator.py:

$ python gdlDataGenerator.py




