import unittest
from jsonschema.exceptions import ValidationError
from src.config.table_config import read_table_config, validate_config


class TestSchemas(unittest.TestCase):

    def test_valid_config_raises_error_when_field_is_unknown(self):
        config = {"unknown": []}
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_valid_config_raises_error_when_field_is_not_list(self):
        config = {"uniqueness": ""}
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_valid_config_raises_error_when_field_schema_is_invalid(self):
        config = {"uniqueness": [{}]}
        with self.assertRaises(ValidationError):
            validate_config(config)

    def test_read_table_config_returns_constraints(self):
        config = read_table_config("tests/resources/test_table_config.json")
        assert config == \
               {
                   "uniqueness": [
                       {
                           "name": "UniquePerson",
                           "criticality": 3,
                           "condition": ["first_name", "last_name"]
                       },
                       {
                           "name": "UniqueCompany",
                           "criticality": 1,
                           "condition": ["siren"]
                       }
                   ]
               }
