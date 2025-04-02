def access_credential_entity(credential) -> dict:
    return {
        "id": str(credential.id),
        "conn_string": str(credential.conn_string),
        "conn_type": str(credential.conn_type),
        "name": str(credential.name),
        "is_active": bool(credential.is_active),
        "created_at": credential.created_at,
        "updated_at": credential.updated_at
    }


def system_entity(system) -> dict:
    return {
        "id": str(system.id),
        "name": str(system.site_name),
        "site_code": str(system.site_code),
        "primary_system": str(system.primary_system)
    }


def access_credential_list_entity(credentials) -> list:
    return [access_credential_entity(credential) for credential in credentials]


def systems_list_entity(systems) -> list:
    return [system_entity(system) for system in systems]
