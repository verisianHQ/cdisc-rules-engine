from typing import Type

from cdisc_rules_engine.interfaces import (
    DataReaderInterface,
    FactoryInterface,
)
from cdisc_rules_engine.services.data_readers.xpt_reader import XPTReader
from cdisc_rules_engine.services.data_readers.dataset_json_reader import (
    DatasetJSONReader,
)
from cdisc_rules_engine.services.data_readers.dataset_ndjson_reader import (
    DatasetNDJSONReader,
)
from cdisc_rules_engine.services.data_readers.parquet_reader import ParquetReader
from cdisc_rules_engine.services.data_readers.usdm_json_reader import USDMJSONReader
from cdisc_rules_engine.enums.dataformat_types import DataFormatTypes


class DataReaderFactory(FactoryInterface):
    _reader_map = {
        DataFormatTypes.XPT.value: XPTReader,
        DataFormatTypes.PARQUET.value: ParquetReader,
        DataFormatTypes.JSON.value: DatasetJSONReader,
        DataFormatTypes.NDJSON.value: DatasetNDJSONReader,
        DataFormatTypes.USDM.value: USDMJSONReader,
    }

    def __init__(
        self,
        service_name: str = None,
        dataset_implementation=None,
        database_config=None,
    ):
        self._default_service_name = service_name
        self.dataset_implementation = dataset_implementation
        self.database_config = database_config

    @classmethod
    def register_service(cls, name: str, service: Type[DataReaderInterface]):
        """
        Registers a new service in internal _service_map
        """
        if not name:
            raise ValueError("Service name must not be empty!")
        if not issubclass(service, DataReaderInterface):
            raise TypeError("Implementation of DataReaderInterface required!")
        cls._reader_map[name] = service

    def get_service(self, name: str = None, **kwargs) -> DataReaderInterface:
        """
        Get instance of service that matches searched implementation
        """
        service_name = name or self._default_service_name
        if service_name in self._reader_map:
            reader = self._reader_map[service_name](self.dataset_implementation)
            # Pass database config to reader
            if self.database_config:
                reader.database_config = self.database_config
            return reader
        raise ValueError(
            f"Service name must be in {list(self._reader_map.keys())}, "
            f"given service name is {service_name}"
        )
