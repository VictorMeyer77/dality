from jsonschema import validate
import json

AVAILABLE_CONFIG_PROPERTIES = ["uniqueness"]

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


def read_table_config(path):
    file = open(path, "r")
    config = json.loads(file.read())
    file.close()
    validate_config(config)
    return config


def validate_config(config):
    for field in config.keys():
        if field not in AVAILABLE_CONFIG_PROPERTIES:
            raise ValueError(f"invalid property {field}, allowed: {AVAILABLE_CONFIG_PROPERTIES}")
        else:
            if not isinstance(config[field], list):
                raise ValueError(f"expected array in {field}")
            else:
                for constraint in config[field]:
                    validate(constraint, globals()[f"{field}_schema"])
