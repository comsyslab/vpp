from importlib import import_module

from vpp.data_acquisition.adapter.rabbit_mq_adapter import RabbitMQAdapter
from vpp.database.entities.data_acquisition_entities import RabbitMQAdapterEntity, FTPAdapterEntity


def get_data_adapter_from_entity(entity, *args):
    if isinstance(entity, RabbitMQAdapterEntity):
        return RabbitMQAdapter(entity, *args)
    elif isinstance(entity, FTPAdapterEntity):
        return FTPAdapterEntity(entity, *args)

    raise ValueError('Unknown DataAdapter entity ' + str(entity))

def get_data_provider_from_entity(entity):
    from vpp.data_acquisition.data_provider import DataProvider
    return DataProvider(entity)

def instantiate_fqn(name, *args):
    class_ = get_class_from_fqn(name)
    return class_(*args)

def get_class_from_fqn(name):
    parts = name.rsplit('.', 1)
    module = import_module(parts[0])
    class_name = parts[1]
    class_ = getattr(module, class_name)
    return class_