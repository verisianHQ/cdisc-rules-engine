from typing import Dict, List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor
import psycopg2
from datetime import datetime

from sql_executor import SQLExecutor
from ..templates.rules_templates import SQLRuleTemplates
from cdisc_rules_engine.services.define_xml_loader import DefineXMLLoader
from cdisc_rules_engine.services.dictionary_validator import SQLDictionaryValidator
from cdisc_rules_engine.services.cache_manager import SQLCacheManager
from cdisc_rules_engine.services.query_optimiser import QueryOptimiser

class IntegratedSQLRulesEngine:
    """ Main engine integrating all SQL-based validation components. """

    def __init__(self, conn_params: Dict[str, str],
                 max_workers: int = 10,
                 use_cache: bool = True):
        self.conn_params = conn_params
        self.executor = SQLExecutor(conn_params)
        self.templates = SQLRuleTemplates()
        self.define_loader = DefineXMLLoader(conn_params)
        self.dict_validator = SQLDictionaryValidator(conn_params)
        self.cache_manager = SQLCacheManager(conn_params) if use_cache else None
        self.optimiser = QueryOptimiser(conn_params)
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)

    def initialise(self) -> None:
        """ Initialise all components. """
        self.executor.setup_database()
        self.dict_validator.setup_dictionary_tables()
        if self.cache_manager:
            self.cache_manager.setup_cache_tables()
        self.logger.info("SQL Rules Engine initialised")

    def load_study_data(self, study_id: str,
                        dataset_paths: List[str],
                        define_xml_path: Optional[str] = None,
                        dictionary_paths: Optional[Dict[str, str]] = None) -> None:
        """ Load all study data into PostgreSQL. """
        start_time = datetime.now()

        datasets = self.executor.load_datasets(study_id, dataset_paths)
        self.logger.info(f"Loaded {len(datasets)} datasets")

        if define_xml_path:
            self.define_loader.load_define_xml(study_id, define_xml_path)
            self.logger.info("Loaded Define XML metadata")

        if dictionary_paths:
            for dict_type, path in dictionary_paths.items():
                self._load_dictionary(dict_type, path)

        self.optimiser.analyse_and_optimise(study_id)

        elapsed = (datetime.now() - start_time).total_seconds()
        self.logger.info(f"Study data loaded in {elapsed:.2f} seconds")

    def validate_study(self, study_id: str,
                       rules: List[Dict],
                       datasets: Optional[List[str]] = None) -> Dict:
        """ Run validation for a study. """
        start_time = datetime.now()
        results = {
            "study_id": study_id,
            "validation_date": start_time.isoformat(),
            "rule_results": [],
            "summary": {
                "total_rules": len(rules),
                "passed_rules": 0,
                "failed_rules": 0,
                "skipped_rules": 0,
                "total_errors": 0
            }
        }

        if not datasets:
            datasets = self._get_study_datasets(study_id)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

            for rule in rules:
                applicable_datasets = self._get_applicable_datasets(
                    rule, datasets
                )

                for dataset in applicable_datasets:
                    future = executor.submit(
                        self._execute_rule,
                        study_id, rule, dataset
                    )
                    futures.append((rule, dataset, future))

            for rule, dataset, future in futures:
                try:
                    rule_result = future.result()
                    results["rule_results"].append(rule_result)

                    if rule_result["status"] == "passed":
                        results["summary"]["passed_rules"] += 1
                    elif rule_result["status"] == "failed":
                        results["summary"]["failed_rules"] += 1
                        results["summary"]["total_errors"] += len(
                            rule_result["errors"]
                        )
                    else:
                        results["summary"]["skipped_rules"] += 1

                except Exception as e:
                    self.logger.error(
                        f"Error executing rule {rule["id"]} "
                        f"on dataset {dataset}: {str(e)}"
                    )
                    results["rule_results"].append({
                        "rule_id": rule["id"],
                        "dataset": dataset,
                        "status": "error",
                        "message": str(e)
                    })

        elapsed = (datetime.now() - start_time).total_seconds()
        results["validation_time"] = f"{elapsed:.2f} seconds"

        if self.cache_manager:
            cleared = self.cache_manager.clear_expired_cache()
            if cleared > 0:
                self.logger.info(f"Cleared {cleared} expired cache entries")

        return results

    def _execute_rule(self, study_id: str,
                      rule: Dict, dataset: str) -> Dict:
        """ Execute a single rule. """
        rule_id = rule["id"]

        cache_key = None
        if self.cache_manager:
            cache_key = self.cache_manager.get_cache_key(
                study_id, rule_id, dataset, rule.get("params", {})
            )
            cached_results = self.cache_manager.get_cached_results(cache_key)
            if cached_results is not None:
                return {
                    "rule_id": rule_id,
                    "dataset": dataset,
                    "status": "failed" if cached_results else "passed",
                    "errors": cached_results,
                    "cached": True
                }

        template = self._get_rule_template(rule["type"])

        query = self._build_query(template, rule, dataset)

        params = {
            "study_id": study_id,
            "rule_id": rule_id,
            "message": rule.get("message", "Validation error"),
            **rule.get("params", {})
        }

        errors = self.executor.execute_query(query, params)

        if self.cache_manager and cache_key:
            self.cache_manager.cache_results(
                cache_key, study_id, rule_id, dataset, errors
            )

        return {
            "rule_id": rule_id,
            "dataset": dataset,
            "status": "failed" if errors else "passed",
            "errors": errors,
            "cached": False
        }

    def _get_rule_template(self, rule_type: str) -> str:
        """ Get SQL template for rule type. """
        template_map = {
            "value_check": self.templates.get_value_check_template(),
            "codelist_check": self.templates.get_codelist_check_template(),
            "consistency_check": self.templates.get_consistency_check_template(),
            "define_metadata_check": self.templates.get_define_metadata_check_template(),
            "meddra_check": self.dict_validator.get_meddra_validation_template()
        }

        return template_map.get(
            rule_type,
            self.templates.get_value_check_template()
        )

    def _build_query(self, template: str, rule: Dict, dataset: str) -> str:
        """ Build SQL query from template and rule. """
        domain = dataset[:2].upper()

        query_params = {
            "schema": "sdtm",
            "dataset": dataset,
            "domain": domain,
            "condition": rule.get("condition", "1=1"),
            **rule.get("query_params", {})
        }

        return template.format(**query_params)

    def _get_study_datasets(self, study_id: str) -> List[str]:
        """ Get all datasets for a study. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT DISTINCT dataset
                            FROM sdtm.study_datasets
                            WHERE study_id = %s
                            ORDER BY dataset
                            """, (study_id,))

                return [row[0] for row in cur.fetchall()]

    def _get_applicable_datasets(self, rule: Dict,
                                 datasets: List[str]) -> List[str]:
        """ Determine which datasets a rule applies to. """
        rule_domains = rule.get("domains", [])

        if not rule_domains or "ALL" in rule_domains:
            return datasets

        applicable = []
        for dataset in datasets:
            dataset_domain = dataset[:2].upper()
            if dataset_domain in rule_domains:
                applicable.append(dataset)

        return applicable

    def _load_dictionary(self, dict_type: str, path: str) -> None:
        """ Load external dictionary data. """
        self.logger.info(f"Loading {dict_type} dictionary from {path}")
        # TODO: This would be implemented based on dictionary type
        # TODO: Implementation would parse dictionary files and load into PostgreSQL

# example
if __name__ == "__main__":
    conn_params = {
        "host": "localhost",
        "database": "cdisc_db",
        "user": "cdisc_user",
        "password": "password"
    }

    engine = IntegratedSQLRulesEngine(conn_params)
    engine.initialise()

    engine.load_study_data(
        study_id="STUDY001",
        dataset_paths=["./data/ae.xpt", "./data/dm.xpt"],
        define_xml_path="./data/define.xml",
        dictionary_paths={
            "meddra": "./dictionaries/meddra_24_0",
            "whodrug": "./dictionaries/whodrug_21Q1"
        }
    )

    rules = [
        {
            "id": "CORE-000001",
            "type": "value_check",
            "domains": ["AE"],
            "condition": "aeser = \"Y\" AND aesev IS NULL",
            "message": "Severity is required for serious adverse events",
            "query_params": {
                "target_columns": "'aeser', aeser, 'aesev', aesev"
            }
        },
        {
            "id": "CORE-000002",
            "type": "codelist_check",
            "domains": ["AE"],
            "params": {
                "codelist_id": "C71113",
                "ct_version": "2021-09-17"
            },
            "query_params": {
                "variable": "aeser"
            }
        }
    ]

    results = engine.validate_study("STUDY001", rules)
    print(f"Validation completed: {results["summary"]}")