def universal_dictionary_config_serializer_entity(config):
    return {
        "universal_dictionary_url": str(config.universal_dictionary_url),
        "universal_dictionary_jwt": str(config.universal_dictionary_jwt),
        "universal_dictionary_update_frequency": str(config.universal_dictionary_update_frequency),
        "created_at": config.created_at,
        "updated_at": config.updated_at
    }
