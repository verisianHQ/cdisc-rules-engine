from typing import Dict, List, Optional
import xml.etree.ElementTree as ET
from dataclasses import dataclass
import psycopg2
from psycopg2.extras import execute_batch

@dataclass
class DefineVariable:
    """ Represents a variable in Define XML metadata. """
    dataset_oid: str
    variable_name: str
    variable_label: str
    variable_type: str
    variable_length: Optional[int]
    variable_role: Optional[str]
    is_required: bool
    has_codelist: bool
    codelist_id: Optional[str]

@dataclass
class DefineDataset:
    """ Represents a dataset in Define XML metadata. """
    dataset_oid: str
    dataset_name: str
    dataset_label: str
    dataset_class: str
    dataset_structure: str
    dataset_domain: str

class DefineXMLLoader:
    """ Loads Define XML metadata into PostgreSQL. """

    def __init__(self, conn_params: Dict[str, str]):
        self.conn_params = conn_params
        self.namespaces = {
            'def': 'http://www.cdisc.org/ns/def/v2.1',
            'odm': 'http://www.cdisc.org/ns/odm/v1.3'
        }

    def load_define_xml(self, study_id: str, define_xml_path: str) -> None:
        """ Load Define XML into PostgreSQL tables. """
        tree = ET.parse(define_xml_path)
        root = tree.getroot()

        datasets = self._extract_datasets(root)
        variables = self._extract_variables(root)

        with psycopg2.connect(**self.conn_params) as conn:
            self._create_define_tables(conn)
            self._insert_datasets(conn, study_id, datasets)
            self._insert_variables(conn, study_id, variables)
            conn.commit()

    def _extract_datasets(self, root: ET.Element) -> List[DefineDataset]:
        """ Extract dataset metadata from Define XML. """
        datasets = []

        for item_group in root.findall('.//odm:ItemGroupDef', self.namespaces):
            if item_group:
                dataset = DefineDataset(
                    dataset_oid=item_group.get('OID'),
                    dataset_name=item_group.get('Name'),
                    dataset_label=item_group.get('def:Label', ''),
                    dataset_class=item_group.get('def:Class', ''),
                    dataset_structure=item_group.get('def:Structure', ''),
                    dataset_domain=item_group.get('Domain', '')
                )
                datasets.append(dataset)
        return datasets

    def _extract_variables(self, root: ET.Element) -> List[DefineVariable]:
        """ Extract variable metadata from Define XML. """
        variables = []

        item_defs = {}
        for item_def in root.findall('.//odm:ItemDef', self.namespaces):
            item_defs[item_def.get('OID')] = item_def

        for item_group in root.findall('.//odm:ItemGroupDef', self.namespaces):
            dataset_oid = item_group.get('OID')

            for item_ref in item_group.findall('odm:ItemRef', self.namespaces):
                item_oid = item_ref.get('ItemOID')
                item_def = item_defs.get(item_oid)

                if item_def is not None:
                    variable = DefineVariable(
                        dataset_oid=dataset_oid,
                        variable_name=item_def.get('Name'),
                        variable_label=item_def.get('def:Label', ''),
                        variable_type=item_def.get('DataType'),
                        variable_length=int(item_def.get('Length', 0)) if item_def.get('Length') else None,
                        variable_role=item_ref.get('Role'),
                        is_required=item_ref.get('Mandatory') == 'Yes',
                        has_codelist=item_def.find('odm:CodeListRef', self.namespaces) is not None,
                        codelist_id=item_def.find('odm:CodeListRef', self.namespaces).get('CodeListOID')
                        if item_def.find('odm:CodeListRef', self.namespaces) is not None else None
                    )
                    variables.append(variable)

        return variables

    @staticmethod
    def _insert_datasets(conn, study_id: str, datasets: List[DefineDataset]) -> None:
        """ Insert dataset metadata into PostgreSQL. """
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO sdtm.define_datasets (
                    study_id, dataset_oid, dataset_name, dataset_label,
                    dataset_class, dataset_structure, dataset_domain
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (study_id, dataset_oid) 
                DO UPDATE SET
                    dataset_name = EXCLUDED.dataset_name,
                    dataset_label = EXCLUDED.dataset_label,
                    dataset_class = EXCLUDED.dataset_class,
                    dataset_structure = EXCLUDED.dataset_structure,
                    dataset_domain = EXCLUDED.dataset_domain,
                    created_at = CURRENT_TIMESTAMP
            """

            dataset_data = [
                (
                    study_id,
                    dataset.dataset_oid,
                    dataset.dataset_name,
                    dataset.dataset_label,
                    dataset.dataset_class,
                    dataset.dataset_structure,
                    dataset.dataset_domain
                )
                for dataset in datasets
            ]
            
            execute_batch(cur, insert_query, dataset_data)

    @staticmethod
    def _insert_variables(conn, study_id: str, variables: List[DefineVariable]) -> None:
        """Insert variable metadata into PostgreSQL."""
        with conn.cursor() as cur:
            insert_query = """
                INSERT INTO sdtm.define_variables (
                    study_id, dataset_oid, variable_name, variable_label,
                    variable_type, variable_length, variable_role,
                    is_required, has_codelist, codelist_id
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (study_id, dataset_oid, variable_name) 
                DO UPDATE SET
                    variable_label = EXCLUDED.variable_label,
                    variable_type = EXCLUDED.variable_type,
                    variable_length = EXCLUDED.variable_length,
                    variable_role = EXCLUDED.variable_role,
                    is_required = EXCLUDED.is_required,
                    has_codelist = EXCLUDED.has_codelist,
                    codelist_id = EXCLUDED.codelist_id,
                    created_at = CURRENT_TIMESTAMP
            """
            
            variable_data = [
                (
                    study_id,
                    variable.dataset_oid,
                    variable.variable_name,
                    variable.variable_label,
                    variable.variable_type,
                    variable.variable_length,
                    variable.variable_role,
                    variable.is_required,
                    variable.has_codelist,
                    variable.codelist_id
                )
                for variable in variables
            ]
            
            execute_batch(cur, insert_query, variable_data)

    @staticmethod
    def _create_define_tables(conn) -> None:
        """
            Create Define XML metadata tables for sdtm datasets,
            variables and their indexes.
        """
        with conn.cursor() as cur:
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS sdtm.define_datasets (
                        study_id VARCHAR(200),
                        dataset_oid VARCHAR(200),
                        dataset_name VARCHAR(200),
                        dataset_label TEXT,
                        dataset_class VARCHAR(200),
                        dataset_structure VARCHAR(200),
                        dataset_domain VARCHAR(200),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (study_id, dataset_oid)
                    );
                """
            )

            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS sdtm.define_variables (
                        study_id VARCHAR(200),
                        dataset_oid VARCHAR(200),
                        variable_name VARCHAR(200),
                        variable_label TEXT,
                        variable_type VARCHAR(50),
                        variable_length INTEGER,
                        variable_role VARCHAR(50),
                        is_required BOOLEAN,
                        has_codelist BOOLEAN,
                        codelist_id VARCHAR(200),
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        PRIMARY KEY (study_id, dataset_oid, variable_name)
                    );
                """
            )

            cur.execute(
                """
                    CREATE INDEX IF NOT EXISTS idx_define_datasets_name
                        ON sdtm.define_datasets(dataset_name);
                """
            )

            cur.execute(
                """
                    CREATE INDEX IF NOT EXISTS idx_define_variables_name
                        ON sdtm.define_variables(variable_name);
                """
            )