from importlib import import_module


def instantiate_fqn(name, *args):
    class_ = get_class_from_fqn(name)
    return class_(*args)


def get_class_from_fqn(name):
    parts = name.rsplit('.', 1)
    module = import_module(parts[0])
    class_name = parts[1]
    class_ = getattr(module, class_name)
    return class_


def get_data_adapter(data_provider, ini_parser):
        adapter_fqn = ini_parser.get_adapter_fqn()

        from vpp.data_acquisition.adapter.rabbit_mq_adapter import RabbitMQAdapter
        rabbit_mq_name = RabbitMQAdapter.__module__ + '.' + RabbitMQAdapter.__name__
        if adapter_fqn == rabbit_mq_name:
            exchange = ini_parser.get_rabbitmq_exchange()
            queue = ini_parser.get_rabbitmq_queue()
            ssl_options = ini_parser.get_rabbitmq_ssl_options()


            return instantiate_fqn(adapter_fqn, data_provider, exchange, queue, ssl_options)
        else:
            print adapter_fqn + " did not match " + rabbit_mq_name