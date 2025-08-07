"""
Data Preprocessor for SDTM and ADaM clinical data.
"""

import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Any

from cdisc_rules_engine.data_service.sql_interface import PostgresQLInterface
from cdisc_rules_engine.data_service.db_cache import DBCache

logger = logging.getLogger(__name__)


class DataPreprocessor:
    """
    Performs preprocessing operations on clinical data.
    Operations should be performed at data ingestion time.
    """

    def __init__(self, postgres_interface: PostgresQLInterface, cache: DBCache):
        self.pgi = postgres_interface
        self.cache = cache
        self._merged_datasets_cache: Set[str] = set()
        self._relrec_catalog: Optional[List[Dict]] = None

    def preprocess_all(self) -> Dict[str, Any]:
        """Execute all preprocessing stages in sequence."""

        logger.info("Starting data preprocessing pipeline")

        results = {
            "split_processing": {},
            "relrec_catalog": {},
            "metadata_updates": {},
            "timestamp": datetime.now().astimezone(),
        }

        results["split_processing"] = self._process_split_datasets()
        results["relrec_catalog"] = self._build_relrec_catalog()
        results["metadata_updates"] = self._update_metadata(results["timestamp"])

        logger.info("Data preprocessing pipeline completed")
        return results

    def process_rule_driven_merges(self, rule_spec: Dict) -> Optional[str]:
        """Perform RELREC merges based on rule specifications."""
        datasets = rule_spec.get("datasets", [])

        for dataset_spec in datasets:
            if dataset_spec.get("domain") == "RELREC":
                return self._perform_relrec_merge(dataset_spec, rule_spec)

        return None

    def _process_split_datasets(self) -> Dict[str, Any]:
        """
        Concatenate split datasets into single logical datasets.
        Identifies datasets that are parts of the same logical domain and
        concatenates them using SQL UNION ALL operations.
        """
        logger.info("Processing split datasets")

        query = """
            SELECT DISTINCT
                dataset_unsplit_name,
                COUNT(DISTINCT dataset_id) as part_count,
                array_agg(DISTINCT dataset_id ORDER BY dataset_id) as dataset_parts
            FROM public.data_metadata
            WHERE dataset_is_split = true
            GROUP BY dataset_unsplit_name, dataset_domain
            HAVING COUNT(DISTINCT dataset_id) > 1
        """

        self.pgi.execute_sql(query)
        split_groups = self.pgi.fetch_all()

        processed_count = 0

        for group in split_groups:
            unsplit_name = group[0]
            part_count = group[1]
            dataset_parts = group[2]

            logger.info(f"Concatenating {part_count} parts for {unsplit_name}")

            self._concatenate_split_parts(unsplit_name, dataset_parts)
            processed_count += 1

        return {
            "groups_processed": processed_count,
            "total_parts_concatenated": sum(int(g[2]) for g in split_groups) if split_groups else 0,
        }

    def _concatenate_split_parts(self, unsplit_name: str, dataset_parts: List[str]) -> None:
        """Concatenate multiple dataset parts into a single table."""
        union_parts = []
        for part in dataset_parts:
            union_parts.append(f"SELECT * FROM public.{part}")

        union_query = " UNION ALL ".join(union_parts)

        # we don't know if these columns exist in every dataset
        # we can't ORDER BY a column that doesn't exist so we use CASE to handle it
        create_query = f"""
            CREATE TABLE IF NOT EXISTS public.{unsplit_name} AS
            WITH concatenated AS (
                {union_query}
            )
            SELECT * FROM concatenated
            ORDER BY
                CASE
                    WHEN EXISTS (SELECT 1 FROM concatenated WHERE usubjid IS NOT NULL LIMIT 1)
                    THEN usubjid
                    ELSE NULL
                END,
                CASE
                    WHEN EXISTS (SELECT 1 FROM concatenated WHERE studyid IS NOT NULL LIMIT 1)
                    THEN studyid
                    ELSE NULL
                END
        """

        self.pgi.execute_sql(create_query)
        logger.info(f"Created concatenated dataset: {unsplit_name}")

    def _build_relrec_catalog(self) -> Dict[str, Any]:
        """
        Build RELREC relationship catalog.
        Parses the RELREC dataset to create an indexed catalog of all possible
        relationships.
        Does NOT perform actual merges at this stage.
        """
        logger.info("Building RELREC catalog")

        check_query = """
            SELECT COUNT(*)
            FROM public.data_metadata
            WHERE dataset_domain = 'RELREC'
            LIMIT 1
        """

        self.pgi.execute_sql(check_query)
        result = self.pgi.fetch_one()
        has_relrec = result[0] > 0 if result else False

        if not has_relrec:
            logger.info("No RELREC dataset found, skipping catalog creation")
            return {"catalog_created": False}

        name_query = """
            SELECT DISTINCT dataset_id
            FROM public.data_metadata
            WHERE dataset_domain = 'RELREC'
            LIMIT 1
        """

        self.pgi.execute_sql(name_query)
        result = self.pgi.fetch_one()
        relrec_table = result[0] if result else None

        if not relrec_table:
            return {"catalog_created": False}

        catalog_query = f"""
            CREATE TABLE IF NOT EXISTS public.relrec_catalog AS
            SELECT
                studyid,
                usubjid,
                relid,
                rdomain,
                idvar,
                idvarval,
                reltype,
                ROW_NUMBER() OVER (ORDER BY studyid, usubjid, relid) as catalog_id
            FROM public.{relrec_table}
        """

        self.pgi.execute_sql(catalog_query)

        index_queries = [
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_rdomain ON public.relrec_catalog(rdomain)",
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_relid ON public.relrec_catalog(relid)",
            "CREATE INDEX IF NOT EXISTS idx_relrec_catalog_studyid_usubjid ON public.relrec_catalog(studyid, usubjid)",
        ]

        for idx_query in index_queries:
            self.pgi.execute_sql(idx_query)

        stats_query = """
            SELECT
                COUNT(DISTINCT relid) as unique_relationships,
                COUNT(DISTINCT rdomain) as unique_domains,
                COUNT(*) as total_records
            FROM public.relrec_catalog
        """

        self.pgi.execute_sql(stats_query)
        result = self.pgi.fetch_one()
        stats = result if result else (0, 0, 0)

        self._cache_available_relrec_merges()

        return {
            "catalog_created": True,
            "unique_relationships": stats[0],
            "unique_domains": stats[1],
            "total_records": stats[2],
        }

    def _cache_available_relrec_merges(self) -> None:
        """
        Cache the list of possible RELREC partners for each domain.
        Updates metadata with available merge information.
        """
        query = """
            WITH domain_pairs AS (
                SELECT DISTINCT
                    r1.rdomain as left_domain,
                    r2.rdomain as right_domain,
                    r1.relid
                FROM public.relrec_catalog r1
                JOIN public.relrec_catalog r2
                    ON r1.studyid = r2.studyid
                    AND r1.usubjid = r2.usubjid
                    AND r1.relid = r2.relid
                WHERE r1.rdomain != r2.rdomain
            )
            SELECT
                left_domain,
                array_agg(DISTINCT right_domain || '_' || relid) as available_merges
            FROM domain_pairs
            GROUP BY left_domain
        """

        self.pgi.execute_sql(query)

        if self.pgi._last_results:
            for domain, merges in self.pgi.fetch_all():
                update_query = """
                    UPDATE public.data_metadata
                    SET
                        contains_relrec_refs = true,
                        available_relrec_merges = %s
                    WHERE dataset_domain = %s
                """
                self.pgi.execute_sql(update_query, (merges, domain))

    def _perform_relrec_merge(self, dataset_spec: Dict, rule_spec: Dict) -> Optional[str]:
        """Perform a specific RELREC merge based on rule requirements."""
        wildcard = dataset_spec.get("wildcard", "__")
        left_domain = rule_spec.get("domains", {}).get("Include", [None])[0]

        if not left_domain:
            logger.warning("No left domain specified for RELREC merge")
            return None

        merge_id = f"relrec_{left_domain}_{wildcard}"

        if merge_id in self._merged_datasets_cache:
            logger.info(f"Using cached merge: {merge_id}. Skipping creation.")
            return merge_id

        # this would be a separate schema file but we require merge_id and left_domain to be unique
        merge_query = f"""
            CREATE TABLE IF NOT EXISTS public.{merge_id} AS
            WITH relrec_pairs AS (
                SELECT
                    r1.*,
                    r2.rdomain as rdomain_right,
                    r2.idvar as idvar_right,
                    r2.idvarval as idvarval_right
                FROM public.relrec_catalog r1
                JOIN public.relrec_catalog r2
                    ON r1.studyid = r2.studyid
                    AND r1.usubjid = r2.usubjid
                    AND r1.relid = r2.relid
                WHERE r1.rdomain = '{left_domain}'
                    AND r1.rdomain != r2.rdomain
            ),
            left_data AS (
                SELECT * FROM public.{left_domain.lower()}
            ),
            merged_data AS (
                SELECT
                    l.*,
                    rp.rdomain_right,
                    rp.relid,
                    rp.idvar_right,
                    rp.idvarval_right
                FROM left_data l
                JOIN relrec_pairs rp
                    ON l.studyid = rp.studyid
                    AND l.usubjid = rp.usubjid
            )
            SELECT * FROM merged_data
        """

        self.pgi.execute_sql(merge_query)

        self._merged_datasets_cache.add(merge_id)
        self._add_merged_dataset_metadata(merge_id, left_domain)

        logger.info(f"Created RELREC merge: {merge_id}")
        return merge_id

    def _add_merged_dataset_metadata(self, merge_id: str, left_domain: str) -> None:
        """Add metadata entries for a newly created merged dataset."""
        # not entirely sure if this datetime format is what we want.
        timestamp = datetime.now().astimezone()

        column_query = f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
                AND table_name = '{merge_id}'
        """

        self.pgi.execute_sql(column_query)
        columns = self.pgi.fetch_all()

        for col_name, col_type in columns:
            insert_query = """
                INSERT INTO public.data_metadata (
                    created_at,
                    updated_at,
                    dataset_filename,
                    dataset_filepath,
                    dataset_id,
                    dataset_name,
                    dataset_domain,
                    dataset_is_split,
                    dataset_unsplit_name,
                    dataset_preprocessed,
                    var_name,
                    var_type,
                    preprocessing_stage
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """

            values = (
                timestamp,
                timestamp,
                f"{merge_id}.xpt",
                "preprocessed",
                merge_id,
                merge_id,
                left_domain,
                False,
                merge_id,
                timestamp,
                col_name.upper(),
                col_type,
                "relrec_merged",
            )

            self.pgi.execute_sql(insert_query, values)

    def _update_metadata(self, timestamp: datetime) -> Dict[str, int]:
        """Update metadata for all preprocessed datasets."""
        logger.info("Updating metadata")

        split_update_query = """
            UPDATE public.data_metadata
            SET
                dataset_preprocessed = %s,
                preprocessing_stage = 'split_processed',
                updated_at = %s
            WHERE dataset_is_split = true
        """

        affected_split = self.pgi.execute_sql(split_update_query, (timestamp, timestamp))

        relrec_update_query = """
            UPDATE public.data_metadata
            SET
                preprocessing_stage = 'relrec_ready',
                updated_at = %s
            WHERE contains_relrec_refs = true
        """

        affected_relrec = self.pgi.execute_sql(relrec_update_query, (timestamp,))

        return {
            "split_datasets_updated": affected_split,
            "relrec_ready_datasets": affected_relrec,
            "total_updated": affected_split + affected_relrec,
        }

    def get_preprocessing_status(self) -> Dict[str, Any]:
        """Get current preprocessing status and statistics."""

        status_query = """
            SELECT
                preprocessing_stage,
                COUNT(DISTINCT dataset_id) as dataset_count,
                COUNT(DISTINCT var_name) as variable_count
            FROM public.data_metadata
            GROUP BY preprocessing_stage
        """

        self.pgi.execute_sql(status_query)

        status = {}
        if self.pgi._last_results:
            for stage, ds_count, var_count in self.pgi._last_results:
                status[stage or "raw"] = {"datasets": ds_count, "variables": var_count}

        status["cached_merges"] = len(self._merged_datasets_cache)

        return status
