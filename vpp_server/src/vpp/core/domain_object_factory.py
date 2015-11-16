from vpp.data_acquisition.rabbit_mq_adapter import RabbitMQAdapter

from vpp.database.entities.data_acquisition_entities import RabbitMQAdapterEntity, DataProcessorEntity, \
    DataInterpreterEntity


def get_domain_object_from_entity(entity):
    if isinstance(entity, RabbitMQAdapterEntity):
        return RabbitMQAdapter(entity)

    elif isinstance(entity, DataProcessorEntity):
        from vpp.data_acquisition import data_processor_async #placed here to avoid circular import
        data_processor_class = getattr(data_processor_async, entity.domain_type)
        return data_processor_class(entity)

    raise ValueError('Unknown entity ' + str(entity))


def get_data_interpreter_from_entity(entity):
    if isinstance(entity, DataInterpreterEntity):
        from vpp.data_acquisition import grundfos_data_interpreter #placed here to avoid circular import
        data_interpreter_class = getattr(grundfos_data_interpreter, entity.domain_type)
        return data_interpreter_class(entity)

    raise ValueError('Unknown entity ' + str(entity))

def get_data_provider_from_entity(entity):
    from vpp.data_acquisition.data_provider import PeriodicDataProvider, CallbackDataProvider
    if entity.interval is None or entity.interval == 0:
        return CallbackDataProvider(entity)
    else:
        return PeriodicDataProvider(entity)


