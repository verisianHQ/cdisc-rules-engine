"""Rule types that check the dataset's own records."""

from typing import Dict, Tuple

from cdisc_rules_engine.compiler.sources.context import SourceContext
from cdisc_rules_engine.compiler.sources.views import ensure_view, literal, typed
from cdisc_rules_engine.metadata.store import DEFINE_DATASETS_TYPE
from cdisc_rules_engine.models.sql.column_schema import SqlColumnSchema


def record_data(context: SourceContext) -> str:
    # nothing to build
    return context.rule_dataset()


def contents_against_define(context: SourceContext) -> str:
    """
    The dataset's records, each carrying its dataset's metadata and the define.xml's
    metadata for it PLUS a flag for whether the define key variables identify the record.
    """
    table_id = context.rule_dataset()
    pgi = context.pgi
    source = pgi.schema.get_table(table_id)
    if not source:
        raise ValueError(f"Table {table_id} not found")
    dataset = context.dataset_metadata
    define_dataset = context.store.define_dataset(dataset)

    # only add what the dataset doesn't already have.
    # A real column called e.g. dataset_name wins over the constant.
    added: Dict[str, str] = {}
    for column in ["dataset_location", "dataset_name", "dataset_label", "dataset_domain"]:
        if not source.get_column(column):
            added[column] = "Char"
    for column, column_type in DEFINE_DATASETS_TYPE.items():
        if not source.get_column(column):
            added[column] = column_type

    values = {
        "dataset_location": dataset.filename,
        "dataset_name": dataset.name,
        "dataset_label": dataset.label,
        "dataset_domain": dataset.domain,
        **{key: value for key, value in define_dataset.items() if key in DEFINE_DATASETS_TYPE},
    }

    columns = [column for _, column in source.get_columns() if column.name != "id"]
    select = ["t.*"]
    for name, column_type in added.items():
        column = SqlColumnSchema.define(name, column_type)
        columns.append(column)
        select.append(f"{typed(literal(values.get(name)), column_type)} AS {column.hash}")

    unique = SqlColumnSchema.define("define_key_sequence_is_unique", "Bool")
    columns.append(unique)
    unique_sql, joins, pivot = _key_sequence_is_unique(context, table_id, define_dataset)
    select.append(f"{unique_sql} AS {unique.hash}")

    body = f"{pivot}SELECT {', '.join(select)} FROM {source.hash} t {joins}".strip()
    return ensure_view(pgi, body, columns, source=source.source)


def _key_sequence_is_unique(context: SourceContext, table_id: str, define_dataset: dict) -> Tuple[str, str, str]:
    """
    Returns (expression, joins, cte) for "do this record's define.xml keys identify it".
    The awkward part is SUPP, yuck.
    A key can be a QNAM, which lives in SUPPxx rather than on the record.
    In that case SUPP is pivoted to one JSONB per (USUBJID, IDVAR, IDVARVAL) and joined
    back on each IDVAR, and that jsonb becomes part of the partition key.
    """
    pgi = context.pgi
    key_sequence = define_dataset.get("define_dataset_key_sequence") or ""
    supp_table_hash = pgi.schema.get_table_hash(f"SUPP{table_id}".lower())

    if not supp_table_hash:
        if not key_sequence:
            # no keys in the define so therefore nothing to be duplicated on
            return "true", "", ""
        return f"(COUNT(*) OVER (PARTITION BY {key_sequence}) = 1)", "", ""

    pgi.execute_sql(
        f"SELECT STRING_AGG(DISTINCT IDVAR, ', ') as idvars, STRING_AGG(DISTINCT QNAM, ', ') as qnams "
        f"from {supp_table_hash};"
    )
    supp_columns = pgi.fetch_one() or {}
    idvar_cols = [col for col in (supp_columns.get("idvars") or "").split(", ") if col]
    qnam_cols = (supp_columns.get("qnams") or "").split(", ")
    parent_keys = [col for col in key_sequence.split(",") if col and col not in qnam_cols]

    if not idvar_cols:
        # a SUPP with no IDVARs can't be joined to records so only the parent's own keys count
        if not parent_keys:
            return "true", "", ""
        return f"(COUNT(*) OVER (PARTITION BY {', '.join(f't.{c}' for c in parent_keys)}) = 1)", "", ""

    pivot = f"""WITH pivoted_supp as (
            SELECT
                STUDYID, RDOMAIN, USUBJID, IDVARVAL, IDVAR,
                jsonb_object_agg(QNAM, QVAL) AS all_supp_vars
            FROM {supp_table_hash}
            GROUP BY 1, 2, 3, 4, 5
        ) """
    joins = "\n".join(
        f"""LEFT JOIN pivoted_supp ps_{col}
            ON t.STUDYID = ps_{col}.STUDYID
            AND t.DOMAIN = ps_{col}.RDOMAIN
            AND t.USUBJID = ps_{col}.USUBJID
            AND ps_{col}.IDVAR = '{col}'
            AND t.{col}::text = ps_{col}.IDVARVAL::text"""
        for col in idvar_cols
    )
    # a record normally only matches SUPP through one of its IDVARs
    # take whichever join hit
    supp_values = f"COALESCE({', '.join(f'ps_{col}.all_supp_vars' for col in idvar_cols)})"
    keys = [*(f"t.{col}" for col in parent_keys), supp_values]
    return f"(COUNT(*) OVER (PARTITION BY {', '.join(keys)}) = 1)", joins, pivot
