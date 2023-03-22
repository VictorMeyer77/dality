import unittest
from jsonschema.exceptions import ValidationError
from src.config.table_config import read_table_config, validate_config


class TestTableConfig(unittest.TestCase):

    def test_valid_config_raises_error_when_dataframe_info_is_missing(self):
        config = {}
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_valid_config_raises_error_when_dataframe_info_schema_is_invalid(self):
        config = {
            "dataframe_info": {
                "env": "prod",
                "source": "postgresql"
            }
        }
        with self.assertRaises(ValidationError):
            validate_config(config)

    def test_valid_config_raises_error_when_constraint_field_is_unknown(self):
        config = {
            "dataframe_info": {
                "env": "prod",
                "source": "postgresql",
                "schema": "website",
                "table": "user"
            },
            "unknown": []
        }
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_valid_config_raises_error_when_constraint_field_is_not_list(self):
        config = {
            "dataframe_info": {
                "env": "prod",
                "source": "postgresql",
                "schema": "website",
                "table": "user"
            },
            "uniqueness": ""
        }
        with self.assertRaises(ValueError):
            validate_config(config)

    def test_valid_config_raises_error_when_constraint_schema_is_invalid(self):
        config = {
            "dataframe_info": {
                "env": "prod",
                "source": "postgresql",
                "schema": "website",
                "table": "user"
            },
            "uniqueness": [{}]
        }
        with self.assertRaises(ValidationError):
            validate_config(config)

    def test_read_table_config_returns_constraints(self):
        config = read_table_config("tests/resources/test_table_config.json")
        assert config == \
               {
                   "dataframe_info": {
                       "env": "prod",
                       "source": "postgresql",
                       "schema": "website",
                       "table": "user"
                   },
                   "uniqueness": [
                       {
                           "name": "UniquenessPerson",
                           "criticality": 2,
                           "condition": ["first_name", "last_name"]
                       },
                       {
                           "name": "UniquenessId",
                           "criticality": 3,
                           "condition": ["id"]
                       }
                   ],
                   "completeness": [
                       {
                           "name": "CompletenessNames",
                           "criticality": 3,
                           "columns": ["first_name", "last_name"],
                           "null_values": ["unknown", ""]
                       },
                       {
                           "name": "CompletenessAge",
                           "criticality": 1,
                           "columns": ["age"]
                       }
                   ],
                   "accuracy": [
                       {
                           "name": "AccuracyAge",
                           "criticality": 2,
                           "condition": "age < 0"
                       }
                   ],
                   "freshness": [
                       {
                           "name": "Freshness",
                           "criticality": 2,
                           "date_column": "created",
                           "days_validity_period": 30
                       }
                   ],
                   "integrity": [
                       {
                           "name": "IntegrityCustomerAddress",
                           "criticality": 1,
                           "left_keys": ["id"],
                           "right_keys": ["id_customer"]
                       },
                       {
                           "name": "IntegrityCustomerAccount",
                           "criticality": 2,
                           "left_keys": ["id", "last_name"],
                           "right_keys": ["id_customer", "last_name"]
                       }
                   ]
               }
