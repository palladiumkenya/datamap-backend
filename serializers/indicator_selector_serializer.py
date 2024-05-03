def indicator_selector_entity(variable) -> dict:
    return {
        "id": str(variable["id"]),
        "indicator": str(variable["indicator"]),
        "baseVariableMappedTo": str(variable["baseVariableMappedTo"]),
        "tablename": variable["created_at"],
        "columnname": variable["columnname"],
        "datatype": variable["datatype"]
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

    print("indicator_entity(indicator) for indicator in indicators",[indicator_entity(indicator) for indicator in indicators], indicators)
    return [indicator_entity(indicator) for indicator in indicators]

