def data_dictionary_entity(dictionary) -> dict:
    return {
        "id": str(dictionary["id"]),
        "datasource_id": str(dictionary["datasource_id"]),
        "name": str(dictionary["name"]),
        "is_published": bool(dictionary["is_published"]),
        "created_at": dictionary["created_at"],
        "updated_at": dictionary["updated_at"],
        "deleted_at": dictionary["deleted_at"]
    }


def data_dictionary_term_entity(dictionary) -> dict:
    return {
        "term_id": str(dictionary["id"]),
        "dictionary": str(dictionary["dictionary"]),
        "term": str(dictionary["term"]),
        "data_type": str(dictionary["data_type"]),
        "is_required": bool(dictionary["is_required"]),
        "term_description": str(dictionary["term_description"]),
        "expected_values": str(dictionary["expected_values"]),
        "is_active": bool(dictionary["is_active"]),
        "created_at": dictionary["created_at"],
        "updated_at": dictionary["updated_at"]
    }


def data_dictionary_usl_entity(dictionary) -> dict:
    return {
        "name": str(dictionary["name"]),
        "id": str(dictionary["id"]),
        "created_at": dictionary["created_at"],
        "updated_at": dictionary["updated_at"]
    }


def data_dictionary_list_entity(dictionaries) -> list:
    return [data_dictionary_entity(dictionary) for dictionary in dictionaries]


def data_dictionary_terms_list_entity(terms) -> list:
    return [data_dictionary_term_entity(term) for term in terms]


def data_dictionary_usl_list_entity(dictionaries) -> list:
    return [data_dictionary_usl_entity(dictionary) for dictionary in dictionaries]
