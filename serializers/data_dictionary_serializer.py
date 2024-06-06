def data_dictionary_entity(dictionary) -> dict:
    return {
        "term_id": str(dictionary["id"]),
        "dictionary_id": str(dictionary["dictionary_id"]),
        "dictionary_name": str(dictionary["name"]),
        "term": str(dictionary["term"]),
        "data_type": str(dictionary["data_type"]),
        "is_required": bool(dictionary["is_required"]),
        "term_description": str(dictionary["term_description"]),
        "expected_values": str(dictionary["expected_values"]),
        "is_active": bool(dictionary["is_active"]),
        "created_at": dictionary["created_at"],
        "updated_at": dictionary["updated_at"]
    }


def data_dictionary_term_entity(dictionary) -> dict:
    return {
        "term_id": str(dictionary["id"]),
        "dictionary_id": str(dictionary["dictionary_id"]),
        "term": str(dictionary["term"]),
        "data_type": str(dictionary["data_type"]),
        "is_required": bool(dictionary["is_required"]),
        "term_description": str(dictionary["term_description"]),
        "expected_values": str(dictionary["expected_values"]),
        "is_active": bool(dictionary["is_active"]),
        "created_at": dictionary["created_at"],
        "updated_at": dictionary["updated_at"]
    }


def data_dictionary_list_entity(dictionaries) -> list:
    return [data_dictionary_entity(dictionary) for dictionary in dictionaries]


def data_dictionary_terms_list_entity(terms) -> list:
    return [data_dictionary_term_entity(term) for term in terms]
