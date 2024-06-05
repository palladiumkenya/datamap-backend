def access_credential_entity(credential) -> dict:
    return {
        "id": str(credential["id"]),
        "conn_string": str(credential["conn_string"]),
        "name": str(credential["name"]),
        "system": str(credential["system"]),
        "system_version": str(credential["system_version"]),
        "is_active": bool(credential["is_active"]),
        "created_at": credential["created_at"],
        "updated_at": credential["updated_at"]
    }


def access_credential_list_entity(credentials) -> list:
    return [access_credential_entity(credential) for credential in credentials]
