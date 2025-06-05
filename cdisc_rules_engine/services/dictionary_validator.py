import psycopg2
from typing import Dict

class SQLDictionaryValidator:
    """ Handles external dictionary validation in PostgreSQL. """

    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params

    def setup_dictionary_tables(self) -> None:
        """
            Create dictionary reference tables and indexes for:
            - meddra
            - whodrug
            - TODO: more to come
        """
        with psycopg2.connect(**self.conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS sdtm.meddra_terms (
                            meddra_version VARCHAR(50),
                            term_type VARCHAR(10),
                            term_code VARCHAR(20),
                            term_name TEXT,
                            parent_code VARCHAR(20),
                            PRIMARY KEY (meddra_version, term_type, term_code)
                        );
                    """
                )

                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS sdtm.whodrug_terms (
                            whodrug_version VARCHAR(50),
                            drug_code VARCHAR(20),
                            drug_name TEXT,
                            atc_code VARCHAR(20),
                            PRIMARY KEY (whodrug_version, drug_code)
                        );
                    """
                )

                cur.execute(
                    """
                        CREATE INDEX IF NOT EXISTS idx_meddra_name
                            ON sdtm.meddra_terms(term_name);

                        CREATE INDEX IF NOT EXISTS idx_whodrug_name
                            ON sdtm.whodrug_terms(drug_name);
                    """
                )

                conn.commit()

    @staticmethod
    def get_meddra_validation_template() -> str:
        """ SQL template for MedDRA validation. """
        return """
               WITH meddra_check AS (
                   SELECT
                       d.*,
                       m_llt.term_code as valid_lltcd,
                       m_pt.term_code as valid_ptcd,
                       CASE
                           WHEN d.{llt_var} IS NULL OR d.{lltcd_var} IS NULL THEN FALSE
                    WHEN m_llt.term_code IS NULL THEN TRUE
                    WHEN UPPER(d.{llt_var}) != UPPER(m_llt.term_name) THEN TRUE
                    ELSE FALSE
                END as has_llt_error,
                CASE 
                    WHEN d.{pt_var} IS NULL OR d.{ptcd_var} IS NULL THEN FALSE
                    WHEN m_pt.term_code IS NULL THEN TRUE
                    WHEN UPPER(d.{pt_var}) != UPPER(m_pt.term_name) THEN TRUE
                    ELSE FALSE
                END as has_pt_error
                   FROM {schema}.{dataset} d
                       LEFT JOIN sdtm.meddra_terms m_llt
                   ON d.{lltcd_var} = m_llt.term_code
                       AND m_llt.term_type = 'LLT'
                       AND m_llt.meddra_version = %(meddra_version)s
                       LEFT JOIN sdtm.meddra_terms m_pt
                       ON d.{ptcd_var} = m_pt.term_code
                       AND m_pt.term_type = 'PT'
                       AND m_pt.meddra_version = %(meddra_version)s
                   WHERE d.study_id = %(study_id)s
               )
               SELECT
                   %(rule_id)s as rule_id,
                   '{dataset}' as dataset,
                   row_number,
                   usubjid,
                   {domain}seq as seq,
                   CASE
                   WHEN has_llt_error THEN
                   jsonb_build_object(
                   '{llt_var}', {llt_var},
                   '{lltcd_var}', {lltcd_var}
                   )
                   WHEN has_pt_error THEN
                   jsonb_build_object(
                   '{pt_var}', {pt_var},
                   '{ptcd_var}', {ptcd_var}
                   )
               END as error_values,
            CASE 
                WHEN has_llt_error THEN 'Invalid MedDRA LLT code/term combination'
                WHEN has_pt_error THEN 'Invalid MedDRA PT code/term combination'
               END as message
        FROM meddra_check
        WHERE has_llt_error = TRUE OR has_pt_error = TRUE
        LIMIT 1000; \
               """