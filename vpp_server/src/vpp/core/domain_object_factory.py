from importlib import import_module


def instantiate_fqn(name, *args, **kwargs):
    class_ = get_class_from_fqn(name)
    return class_(*args, **kwargs)


def get_class_from_fqn(name):
    parts = name.rsplit('.', 1)
    module = import_module(parts[0])
    class_name = parts[1]
    class_ = getattr(module, class_name)
    return class_


def get_data_adapter(data_provider, data_provider_config):
    adapter_fqn = data_provider_config.adapter_fqn
    return instantiate_fqn(adapter_fqn, data_provider, data_provider_config)


