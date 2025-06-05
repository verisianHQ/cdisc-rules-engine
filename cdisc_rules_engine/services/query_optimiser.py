import psycopg2
from typing import Dict, List
import logging

class QueryOptimiser:
    """ Optimises SQL queries for rule execution. """

    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params
        self.logger = logging.getLogger(__name__)

    def analyse_and_optimise(self, study_id: str) -> None:
        """ Analyse tables and create optimised indexes. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                            SELECT DISTINCT dataset
                            FROM sdtm.study_datasets
                            WHERE study_id = %s
                            """, (study_id,))

                datasets = [row[0] for row in cur.fetchall()]

                for dataset in datasets:
                    self._optimise_dataset(cur, dataset)
                    cur.execute("ANALYSE sdtm.%s" % dataset)

                conn.commit()

    def _optimise_dataset(self, cursor, dataset: str) -> None:
        """ Create optimal indexes for a dataset. """
        standard_indexes = [
            ('study_id', 'study_id'),
            ('usubjid', 'usubjid'),
            ('seq', f'{dataset.lower()}seq'),
            ('study_usubjid', 'study_id, usubjid')
        ]

        for idx_name, columns in standard_indexes:
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{dataset}_{idx_name}
                    ON sdtm.{dataset}({columns})
                """)
            except psycopg2.Error:
                self.logger.warning(f"Could not create index idx_{dataset}_{idx_name}")

        self._create_partial_indexes(cursor, dataset)

    def _create_partial_indexes(self, cursor, dataset: str) -> None:
        """ Create partial indexes for common query patterns. """
        common_vars = self._get_common_variables(dataset)

        for var in common_vars:
            try:
                cursor.execute(f"""
                    CREATE INDEX IF NOT EXISTS idx_{dataset}_{var}_notnull
                    ON sdtm.{dataset}({var})
                    WHERE {var} IS NOT NULL
                """)
            except psycopg2.Error:
                pass

    def get_query_plan(self, query: str, params: Dict) -> str:
        """ Get execution plan for a query. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN ANALYSE {query}", params)
                plan = '\n'.join([row[0] for row in cur.fetchall()])
                return plan

    @staticmethod
    def _get_common_variables(dataset: str) -> List[str]:
        """ Get commonly queried variables for optimisation. """
        common_patterns = {
            'ae': ['aeterm', 'aestdtc', 'aeendtc', 'aeser'],
            'cm': ['cmtrt', 'cmstdtc', 'cmendtc', 'cmdecod'],
            'dm': ['sex', 'race', 'ethnic', 'dmdtc'],
            'ex': ['extrt', 'exdose', 'exdosu', 'exstdtc'],
            'lb': ['lbtestcd', 'lborres', 'lborresu', 'lbstresn']
        }

        return common_patterns.get(dataset.lower(), [])
