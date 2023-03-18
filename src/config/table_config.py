from jsonschema import validate
import json

uniqueness_schema = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "criticality": {
            "type": "number",
            "enum": [1, 2, 3]
        },
        "condition": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems": 1,
            "additionalItems": False,
        },
    },
    "required": ["name", "criticality", "condition"],
    "additionalProperties": False,
}

completeness_schema = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "criticality": {
            "type": "number",
            "enum": [1, 2, 3]
        },
        "columns": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems": 1,
            "additionalItems": False,
        },
        "null_values": {
            "type": "array",
            "items": {
                "type": "string"
            },
            "minItems": 0,
            "additionalItems": False,
        },
    },
    "required": ["name", "criticality", "columns"],
    "additionalProperties": False,
}

accuracy_schema = {
    "type": "object",
    "properties": {
        "name": {
            "type": "string"
        },
        "criticality": {
            "type": "number",
            "enum": [1, 2, 3]
        },
        "condition": {
            "type": "string",
        },
    },
    "required": ["name", "criticality", "condition"],
    "additionalProperties": False,
}


def read_table_config(path):
    file = open(path, "r")
    config = json.loads(file.read())
    file.close()
    validate_config(config)
    return config


def validate_config(config):
    available_config_properties = list(map(lambda x: x.replace("_schema", ""),
                                           filter(lambda x: "_schema" in x, globals().keys())))
    for field in config.keys():
        if field not in available_config_properties:
            raise ValueError(f"invalid property {field}, allowed: {available_config_properties}")
        else:
            if not isinstance(config[field], list):
                raise ValueError(f"expected array in {field}")
            else:
                for constraint in config[field]:
                    validate(constraint, globals()[f"{field}_schema"])
