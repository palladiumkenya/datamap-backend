import json


def universal_dictionary_facility_pulls_serializer_entity(facility_pull) -> dict:
    return {
        "facility_mfl_code": str(facility_pull["facility_mfl_code"]),
        "date_last_updated": str(facility_pull["date_last_updated"]),
        "dictionary_versions": json.loads(facility_pull["dictionary_versions"])
    }


def universal_dictionary_facility_pulls_serializer_list(facility_pulls) -> list:
    return [universal_dictionary_facility_pulls_serializer_entity(pull) for pull in facility_pulls]
