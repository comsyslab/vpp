from vpp.data_acquisition.rabbit_mq_adapter import RabbitMQAdapter

from vpp.database.entities.data_acquisition_entities import RabbitMQAdapterEntity, DataProcessorEntity, \
    DataInterpreterEntity
from importlib import import_module

def get_domain_object_from_entity(entity):
    if isinstance(entity, RabbitMQAdapterEntity):
        return RabbitMQAdapter(entity)

    elif isinstance(entity, DataProcessorEntity):
        name = entity.domain_type
        class_ = get_class_from_fqn(name)
        return class_(entity)

    raise ValueError('Unknown entity ' + str(entity))

def get_data_interpreter_from_entity(entity):
    if not isinstance(entity, DataInterpreterEntity):
        raise ValueError('Unknown entity ' + str(entity))

    class_name = entity.domain_type
    class_ = get_class_from_fqn(class_name)
    return class_(entity)


def get_data_provider_from_entity(entity):
    from vpp.data_acquisition.data_provider import PeriodicDataProvider, CallbackDataProvider
    if entity.interval is None or entity.interval == 0:
        return CallbackDataProvider(entity)
    else:
        return PeriodicDataProvider(entity)


def get_class_from_fqn(name):
    parts = name.rsplit('.', 1)
    module = import_module(parts[0])
    class_name = parts[1]
    class_ = getattr(module, class_name)
    return class_