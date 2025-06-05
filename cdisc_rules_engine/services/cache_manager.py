import psycopg2
import hashlib
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

class SQLCacheManager:
    """ Manages caching for SQL rule execution. """

    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params
        self.cache_ttl = timedelta(hours=24)

    def setup_cache_tables(self) -> None:
        """ Create cache tables. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS sdtm.rule_cache (
                           cache_key VARCHAR(64) PRIMARY KEY,
                           study_id VARCHAR(200),
                           rule_id VARCHAR(50),
                           dataset VARCHAR(50),
                           result_count INTEGER,
                           results JSONB,
                           created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                           expires_at TIMESTAMP
                        );

                        CREATE INDEX IF NOT EXISTS idx_cache_study_rule
                            ON sdtm.rule_cache(study_id, rule_id);

                        CREATE INDEX IF NOT EXISTS idx_cache_expires
                            ON sdtm.rule_cache(expires_at);
                    """
                )
                conn.commit()

    @staticmethod
    def get_cache_key(study_id: str, rule_id: str,
                      dataset: str, params: Dict[str, Any]) -> str:
        """ Generate cache key for rule execution. """
        cache_data = {
            'study_id': study_id,
            'rule_id': rule_id,
            'dataset': dataset,
            'params': params
        }
        cache_str = json.dumps(cache_data, sort_keys=True)
        return hashlib.sha256(cache_str.encode()).hexdigest()

    def get_cached_results(self, cache_key: str) -> Optional[List[Dict]]:
        """ Retrieve cached results if available. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        SELECT results
                        FROM sdtm.rule_cache
                        WHERE cache_key = %s
                          AND expires_at > CURRENT_TIMESTAMP
                    """,
                    (cache_key,)
                )

                row = cur.fetchone()
                if row:
                    return row[0]
                return None

    def cache_results(self, cache_key: str, study_id: str,
                      rule_id: str, dataset: str,
                      results: List[Dict]) -> None:
        """ Cache rule execution results. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                expires_at = datetime.now() + self.cache_ttl

                cur.execute(
                    """
                        INSERT INTO sdtm.rule_cache
                        (cache_key, study_id, rule_id, dataset,
                         result_count, results, expires_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (cache_key)
                            DO UPDATE SET
                                          result_count = EXCLUDED.result_count,
                                          results = EXCLUDED.results,
                                          created_at = CURRENT_TIMESTAMP,
                                          expires_at = EXCLUDED.expires_at
                    """,
                    (cache_key, study_id, rule_id, dataset,
                        len(results), json.dumps(results), expires_at)
                )
                conn.commit()

    def clear_expired_cache(self) -> int:
        """ Remove expired cache entries. """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        DELETE FROM sdtm.rule_cache
                        WHERE expires_at < CURRENT_TIMESTAMP
                    """
                )
                deleted = cur.rowcount
                conn.commit()
                return deleted