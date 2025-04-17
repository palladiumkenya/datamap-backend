def site_config_serializer_entity(config) -> dict:
    return {
        "site_name": str(config.site_name),
        "site_code": str(config.site_code),
        "primary_system": str(config.primary_system),
        "country": str(config.country),
        "region": str(config.region),
        "organization": str(config.organization)

    }


def site_config_list_entity(configs) -> list:
    return [site_config_serializer_entity(config) for config in configs]
