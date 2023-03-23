import unittest
from src.config.table_config import read_table_config
from src.qualities import *
from pyspark.sql import SparkSession
from datetime import datetime, timedelta

spark = SparkSession.builder.getOrCreate()


class TestQualities(unittest.TestCase):

    def test_uniqueness_quality_returns_duplicate_count(self):
        dataset = [("Xavier", "Dupont", 34),
                   ("Mathilde", "Martin", 49),
                   ("Matthieu", "Timbert", 15),
                   ("Xavier", "Dupont", 68),
                   ("Matthieu", "Timbert", 34)]
        schema = ["first_name", "last_name", "age"]
        input_df = spark.createDataFrame(data=dataset, schema=schema)
        uniqueness_config = [
            {"name": "UniquePerson", "criticality": 3, "condition": ["first_name", "last_name"]},
            {"name": "UniqueAge", "criticality": 1, "condition": ["age"]},
        ]
        target_result = spark.createDataFrame(
            data=[("uniqueness", "UniquePerson", 3, 2), ("uniqueness", "UniqueAge", 1, 1)],
            schema=RESULT_SCHEMA
        )
        result = uniqueness_quality(spark, input_df, uniqueness_config)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_completeness_quality_returns_missing_count(self):
        dataset = [("Xavier", "unknown", 34),
                   ("Mathilde", "", 49),
                   ("Matthieu", None, 15),
                   ("Xavier", "Dupont", 68),
                   ("Matthieu", "Timbert", None)]
        schema = ["first_name", "last_name", "age"]
        input_df = spark.createDataFrame(data=dataset, schema=schema)
        completeness_config = [
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
            },
        ]
        target_result = spark.createDataFrame(
            data=[("completeness", "CompletenessNames", 3, 3), ("completeness", "CompletenessAge", 1, 1)],
            schema=RESULT_SCHEMA
        )
        result = completeness_quality(spark, input_df, completeness_config)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_accuracy_quality_returns_match_count(self):
        dataset = [("productA", 10.0, 15.0),
                   ("productB", 9.0, 4.5),
                   (" productC", -0.1, 15.0),
                   (" productD", 10.0, 8.0),
                   (" productE", 3.98, 5.98)]
        schema = ["name", "price_excl_tax", "price_incl_tax"]
        input_df = spark.createDataFrame(data=dataset, schema=schema)
        accuracy_config = [
            {
                "name": "AccuracyPrice",
                "criticality": 3,
                "condition": "price_excl_tax < 0.0 OR price_excl_tax < price_incl_tax"
            },
            {
                "name": "AccuracyProduct",
                "criticality": 1,
                "condition": "name LIKE ' %'"
            }
        ]
        target_result = spark.createDataFrame(
            data=[("accuracy", "AccuracyPrice", 3, 3), ("accuracy", "AccuracyProduct", 1, 3)],
            schema=RESULT_SCHEMA
        )
        result = accuracy_quality(spark, input_df, accuracy_config)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_freshness_quality_returns_obsolete_count(self):
        dataset = [("Xavier", "Dupont", 34, datetime.now() - timedelta(days=25)),
                   ("Mathilde", "Martin", 49, datetime.now() - timedelta(days=34)),
                   ("Matthieu", "Timbert", 15, datetime.now() - timedelta(days=22)),
                   ("Xavier", "Dupont", 68, datetime.now() - timedelta(days=45)),
                   ("Matthieu", "Timbert", 34, datetime.now() - timedelta(days=100))]
        schema = ["first_name", "last_name", "age", "created"]
        input_df = spark.createDataFrame(data=dataset, schema=schema)
        freshness_config = [
            {
                "name": "Freshness",
                "criticality": 2,
                "date_column": "created",
                "days_validity_period": 30
            }
        ]
        target_result = spark.createDataFrame(
            data=[("freshness", "Freshness", 2, 3)],
            schema=RESULT_SCHEMA
        )
        result = freshness_quality(spark, input_df, freshness_config)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_integrity_quality_returns_missing_relation_count(self):
        dataset_customers = [("Xavier", "Dupont", 34676),
                             ("Mathilde", "Martin", 49878),
                             ("Matthieu", "Timbert", 15112),
                             ("Henri", "Balert", 68087),
                             ("Louise", "Derit", 34097)]
        dataset_addresses = [("34 rue grande", "Marseille", 15112),
                             ("12 avenue thiers", "Nantes", 34097)]
        dataset_accounts = [(1928.99, 68087, "Balert")]
        employees_input_df = spark.createDataFrame(data=dataset_customers, schema=["first_name", "last_name", "id"])
        addresses_input_df = spark.createDataFrame(data=dataset_addresses, schema=["address", "city", "id_customer"])
        accounts_input_df = spark.createDataFrame(data=dataset_accounts, schema=["amount", "id_customer", "last_name"])
        integrity_config = [
            {
                "name": "IntegrityCustomerAddress",
                "criticality": 2,
                "left_keys": ["id"],
                "right_keys": ["id_customer"],
            },
            {
                "name": "IntegrityCustomerAccount",
                "criticality": 1,
                "left_keys": ["id", "last_name"],
                "right_keys": ["id_customer", "last_name"],
            }
        ]
        target_result = spark.createDataFrame(
            data=[("integrity", "IntegrityCustomerAddress", 2, 3), ("integrity", "IntegrityCustomerAccount", 1, 4)],
            schema=RESULT_SCHEMA
        )
        result = integrity_quality(spark, employees_input_df, [addresses_input_df, accounts_input_df], integrity_config)
        assert result.schema == target_result.schema and result.collect() == target_result.collect()

    def test_integrity_quality_raises_error_when_key_not_in_table(self):
        dataset_customers = [("Xavier", "Dupont", 34676),
                             ("Mathilde", "Martin", 49878),
                             ("Matthieu", "Timbert", 15112),
                             ("Henri", "Balert", 68087),
                             ("Louise", "Derit", 34097)]
        dataset_addresses = [("34 rue grande", "Marseille", 15112),
                             ("12 avenue thiers", "Nantes", 34097)]
        employees_input_df = spark.createDataFrame(data=dataset_customers, schema=["first_name", "last_name", "id"])
        addresses_input_df = spark.createDataFrame(data=dataset_addresses, schema=["address", "city", "id_customer"])
        invalid_left_config = [
            {
                "name": "IntegrityCustomerAddress",
                "criticality": 2,
                "left_keys": ["invalid"],
                "right_keys": ["id_customer"],
            }
        ]
        invalid_right_config = [
            {
                "name": "IntegrityCustomerAddress",
                "criticality": 2,
                "left_keys": ["id"],
                "right_keys": ["invalid"],
            }
        ]
        with self.assertRaises(ValueError):
            integrity_quality(spark, employees_input_df, [addresses_input_df], invalid_left_config)
        with self.assertRaises(ValueError):
            integrity_quality(spark, employees_input_df, [addresses_input_df], invalid_right_config)

    def test_integrity_quality_raises_error_when_constraints_count_mismatch_with_tables_count(self):
        integrity_config = [
            {
                "name": "IntegrityCustomerAccount",
                "criticality": 1,
                "left_keys": ["id", "last_name"],
                "right_keys": ["last_name"],
            }
        ]
        with self.assertRaises(ValueError):
            integrity_quality(spark, None, [], integrity_config)

    def test_integrity_quality_raises_error_when_keys_len_mismatch(self):
        integrity_config = [
            {
                "name": "IntegrityCustomerAccount",
                "criticality": 1,
                "left_keys": ["id", "last_name"],
                "right_keys": ["last_name"],
            }
        ]
        with self.assertRaises(ValueError):
            integrity_quality(spark, None, [None], integrity_config)

    def test_compute_qualities_gather_all_violation_constraint(self):
        customer_dataset = [(1526262, "Xavier", "Dupont", 34, datetime.now() - timedelta(days=100)),
                            (172625363, "Mathilde", "Martin", 49, datetime.now() - timedelta(days=25)),
                            (6152727282, "Matthieu", "Timbert", 15, datetime.now() - timedelta(days=25)),
                            (8928637339, "Xavier", "Dupont", 68, datetime.now() - timedelta(days=25)),
                            (172625363, "Matthieu", "Timbert", 34, datetime.now() - timedelta(days=25)),
                            (1526262, "Maxime", "Laurent", -19, datetime.now() - timedelta(days=25)),
                            (9209381, "Louis", "Pernaud", 41, datetime.now() - timedelta(days=100)),
                            (9226393, "Hervé", "Legrand", 55, datetime.now() - timedelta(days=100)),
                            (827363730, "Matthieu", "Timbert", -7, datetime.now() - timedelta(days=14)),
                            (8172635339, "Cécile", "Henard", 24, datetime.now() - timedelta(days=14)),
                            (9182903, "unknown", "Fayol", 92, datetime.now() - timedelta(days=14)),
                            (928292, "Matthieu", "", None, datetime.now() - timedelta(days=14)),
                            (718272320, "", "unknown", 62, datetime.now() - timedelta(days=14))]
        address_customer = [(172625363, "34 rue grande", "Marseille"),
                            (1526262, "18 avenue thiers", "Nantes"),
                            (9209381, "74 boulevard saint antoine", "Bordeaux"),
                            (9226393, "177 rue paul cezanne", "Paris"),
                            (81928637, "82 place de l'étoile", "Caen")]
        account_customer = [(9209381, "Pernaud", 92000),
                            (928292, "Timbert", 15000)]
        customer_df = spark.createDataFrame(data=customer_dataset,
                                            schema=["id", "first_name", "last_name", "age", "created"])
        address_df = spark.createDataFrame(data=address_customer,
                                           schema=["id_customer", "address", "town"])
        account_df = spark.createDataFrame(data=account_customer,
                                           schema=["id_customer", "last_name", "balance"])
        config = read_table_config("tests/resources/test_table_config.json")
        result = compute_qualities(spark, customer_df, [address_df, account_df], config)
        target_result = spark.createDataFrame(
            data=[("uniqueness", "UniquenessPerson", 2, 2),
                  ("uniqueness", "UniquenessId", 3, 2),
                  ("completeness", "CompletenessNames", 3, 3),
                  ("completeness", "CompletenessAge", 1, 1),
                  ("accuracy", "AccuracyAge", 2, 2),
                  ("freshness", "Freshness", 2, 3),
                  ("integrity", "IntegrityCustomerAddress", 1, 7),
                  ("integrity", "IntegrityCustomerAccount", 2, 12)],
            schema=RESULT_SCHEMA
        )
        assert result.schema == target_result.schema and result.collect() == target_result.collect()
