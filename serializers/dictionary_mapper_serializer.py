def indicator_selector_entity(variable) -> dict:
    return {
        "id": str(variable["id"]),
        "base_repository": str(variable["base_repository"]),
        "base_variable_mapped_to": str(variable["base_variable_mapped_to"]),
        "tablename": variable["tablename"],
        "columnname": variable["columnname"],
        "datatype": variable["datatype"],
        "join_by": variable["join_by"],
        "created_at": str(variable["created_at"]),
        "updated_at": str(variable["updated_at"])

    }


def indicator_selector_list_entity(variables) -> list:
    return [indicator_selector_entity(variable) for variable in variables]


def indicator_entity(indicator) -> dict:
    return {
        "indicator": str(indicator["indicator"]),
        "indicator_value": str(indicator["indicator_value"]),
        "indicator_date": str(indicator["indicator_date"])
    }


def indicator_list_entity(indicators) -> list:

    return [indicator_entity(indicator) for indicator in indicators]



