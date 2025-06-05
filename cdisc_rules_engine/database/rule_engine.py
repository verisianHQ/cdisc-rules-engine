import json
import uuid
from typing import Dict, List, Optional
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from cdisc_rules_engine.database.connection import db

logger = logging.getLogger(__name__)

class RuleEngine:
    """ SQL-based rule execution engine. """
    def __init__(self, max_workers: int = 10):
        self.max_workers = max_workers

    def execute_validation(self, dataset_ids: List[int],
                           rule_ids: Optional[List[int]] = None) -> str:
        """ Execute validation for datasets against rules. """
        execution_id = str(uuid.uuid4())

        if rule_ids:
            rules = self._get_rules_by_ids(rule_ids)
        else:
            rules = self._get_all_rules()

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []

            for dataset_id in dataset_ids:
                for rule in rules:
                    future = executor.submit(
                        self._execute_single_rule,
                        execution_id, dataset_id, rule
                    )
                    futures.append(future)

            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Rule execution failed: {e}")

        return execution_id

    def _execute_single_rule(self, execution_id: str,
                             dataset_id: int, rule: Dict):
        """ Execute a single rule against a dataset. """
        try:
            if rule['sql_template']:
                results = db.execute_query(
                    rule['sql_template'],
                    (dataset_id,)
                )

                if results:
                    self._store_validation_results(
                        execution_id, rule['rule_id'],
                        dataset_id, results
                    )
                else:
                    self._store_success_result(
                        execution_id, rule['rule_id'], dataset_id
                    )

        except Exception as e:
            logger.error(f"Error executing rule {rule['core_id']}: {e}")
            self._store_error_result(
                execution_id, rule['rule_id'],
                dataset_id, str(e)
            )

    @staticmethod
    def _get_rules_by_ids(rule_ids: List[int]) -> List[Dict]:
        """ Get rules by IDs. """
        query = """
                SELECT rule_id, core_id, rule_type, sql_template
                FROM cdisc.rules
                WHERE rule_id = ANY(%s) \
                """
        return db.execute_query(query, (rule_ids,))

    @staticmethod
    def _get_all_rules() -> List[Dict]:
        """ Get all active rules. """
        query = """
                SELECT rule_id, core_id, rule_type, sql_template
                FROM cdisc.rules
                WHERE sql_template IS NOT NULL \
                """
        return db.execute_query(query)

    @staticmethod
    def _store_validation_results(execution_id: str, rule_id: int,
                                  dataset_id: int, results: List[Dict]):
        """ Store validation errors. """
        params_list = []

        for result in results:
            params_list.append((
                rule_id, dataset_id, execution_id, 'ERROR',
                result.get('row_number'),
                result.get('variables', []),
                result.get('error_message', 'Validation failed'),
                json.dumps(result)
            ))

        db.execute_many(
            """INSERT INTO cdisc.validation_results
               (rule_id, dataset_id, execution_id, status,
                row_number, variables, error_message, error_details)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)""",
            params_list
        )

    @staticmethod
    def _store_success_result(execution_id: str, rule_id: int,
                              dataset_id: int):
        """ Store successful validation result. """
        db.execute_update(
            """INSERT INTO cdisc.validation_results
                   (rule_id, dataset_id, execution_id, status, error_message)
               VALUES (%s, %s, %s, %s, %s)""",
            (rule_id, dataset_id, execution_id, 'SUCCESS',
             'Validation passed')
        )

    @staticmethod
    def _store_error_result(execution_id: str, rule_id: int,
                            dataset_id: int, error: str):
        """ Store execution error. """
        db.execute_update(
            """INSERT INTO cdisc.validation_results
                   (rule_id, dataset_id, execution_id, status, error_message)
               VALUES (%s, %s, %s, %s, %s)""",
            (rule_id, dataset_id, execution_id, 'EXECUTION_ERROR', error)
        )