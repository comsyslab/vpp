from vpp.data_acquisition.rabbit_mq_listening_adapter import RabbitMQListeningAdapter


from importlib import import_module

from vpp.database.entities.data_acquisition_entities import RabbitMQAdapterEntity


def get_data_adapter_from_entity(entity):
    if isinstance(entity, RabbitMQAdapterEntity):
        return RabbitMQListeningAdapter(entity)

    raise ValueError('Unknown DataAdapter entity ' + str(entity))

def get_data_provider_from_entity(entity):
    class_ = get_class_from_fqn(entity.domain_type)
    return class_(entity)


def instantiate_fqn(name, *args):
    class_ = get_class_from_fqn(name)
    return class_(*args)

def get_class_from_fqn(name):
    parts = name.rsplit('.', 1)
    module = import_module(parts[0])
    class_name = parts[1]
    class_ = getattr(module, class_name)
    return class_