def access_credential_entity(credential) -> dict:
    return {
        "id": str(credential["id"]),
        "conn_string": str(credential["conn_string"]),
        "is_active": bool(credential["is_active"]),
        "created_at": credential["created_at"]
    }


def access_credential_list_entity(credentials) -> list:
    return [access_credential_entity(credential) for credential in credentials]
