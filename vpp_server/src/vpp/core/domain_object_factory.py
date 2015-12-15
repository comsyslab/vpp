from importlib import import_module


from vpp.util import util


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
        rabbit_mq_adapter_fqn = util.get_fully_qualified_name(RabbitMQAdapter)
        if adapter_fqn == rabbit_mq_adapter_fqn :
            return create_rabbit_mq_adapter(adapter_fqn, data_provider, ini_parser)

        from vpp.data_acquisition.adapter.ftp_adapter import FTPAdapter
        ftp_adapter_fqn = util.get_fully_qualified_name(FTPAdapter)
        if adapter_fqn == ftp_adapter_fqn:
            return create_ftp_adapter(adapter_fqn, data_provider, ini_parser)


        print adapter_fqn + " did not match " + ftp_adapter_fqn


def create_rabbit_mq_adapter(adapter_fqn, data_provider, ini_parser):
    exchange = ini_parser.get_rabbitmq_exchange()
    queue = ini_parser.get_rabbitmq_queue()
    ssl_options = ini_parser.get_rabbitmq_ssl_options()
    adapter = instantiate_fqn(adapter_fqn, data_provider, exchange, queue, ssl_options)
    return adapter


def create_ftp_adapter(adapter_fqn, data_provider, ini_parser):
    ftp_config = ini_parser.get_ftp_config()
    adapter = instantiate_fqn(adapter_fqn, data_provider, ftp_config)
    return adapter