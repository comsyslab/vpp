from vpp.data_acquisition.data_adapter import RabbitMQAdapter
from vpp.database.entities.dataprovider_entities import RabbitMQAdapterEntity, DataProcessorEntity, \
    DataInterpreterEntity


def get_domain_object_from_entity(entity):
    if isinstance(entity, RabbitMQAdapterEntity):
        return RabbitMQAdapter(entity)

    elif isinstance(entity, DataProcessorEntity):
        from vpp.data_acquisition import data_processor #placed here to avoid circular import
        data_processor_class = getattr(data_processor, entity.domain_type)
        return data_processor_class(entity)

    elif isinstance(entity, DataInterpreterEntity):
        from vpp.data_acquisition import data_interpreter #placed here to avoid circular import
        data_processor_class = getattr(data_interpreter, entity.domain_type)
        return data_processor_class(entity)

    raise ValueError('Unknown entity ' + str(entity))


