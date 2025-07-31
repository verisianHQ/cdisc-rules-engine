from typing import TypedDict, Union


class DBTableCache(TypedDict):
    db_table: str
    # key = constructed column_name, value = column name in DB
    columns: dict[str, str]


class DBCache:

    def __init__(self, cache: list[str]):
        self.cache = cache

    @classmethod
    def from_metadata_dict(cls, data_metadata: list[dict]) -> "DBCache":
        cache = {}
        if len(data_metadata) > 0:
            for row in data_metadata:
                table = row.get("dataset_id")
                col = row.get("var_name")
                if table not in cache.keys():
                    cache[table] = DBTableCache(db_table=table, columns={col: col})
                else:
                    cache.get(table).get("columns")[col] = col
        return cls(cache)

    def get_tables(self) -> dict:
        return {k: v["db_table"] for k, v in self.cache.items()}

    def get_db_table_cache(self, table_key: str) -> DBTableCache:
        return self.cache.get(table_key)

    def get_db_table(self, table_key: str) -> Union[str, None]:
        return self.cache.get(table_key).get("db_table", None)

    def get_columns(self, table_key: str) -> dict:
        return self.cache[table_key].get("columns")

    def get_db_column(self, table_key: str, column_key: str) -> Union[str, None]:
        return self.cache.get(table_key).get("columns").get(column_key, None)
