def transformation_serializer(transformation):
    return {
        'id': str(transformation.id),
        'base_table_name': transformation.base_table_name,
        'base_table_column': transformation.base_table_column,
        "previous_value": transformation.previous_value,
        'new_value': transformation.new_value,
        'dictionary_version': transformation.dictionary_version,
        'created_at': transformation.created_at,
        'updated_at': transformation.updated_at
    }


def transformation_list_serializer(transformations):
    return [transformation_serializer(transformation) for transformation in transformations]
