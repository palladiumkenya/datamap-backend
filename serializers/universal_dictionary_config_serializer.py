def universal_dictionary_config_serializer_entity(config):
    return {
        "id": str(config.get("id")),
        "universal_dictionary_url": str(config.get("universal_dictionary_url")),
        "universal_dictionary_jwt": str(config.get("universal_dictionary_jwt")),
        "universal_dictionary_update_frequency": str(config.get("universal_dictionary_update_frequency")),
        "created_at": config.get("created_at"),
        "updated_at": config.get("updated_at")
    }
