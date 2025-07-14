from cdisc_rules_engine.operations.base_operation import BaseOperation


# VERISIAN: SQL REFACTOR COMPLETE
class DomainLabel(BaseOperation):
    def _execute_operation(self):
        """
        Return domain label from standard metadata for self.params.domain (currently executing domain).
        """
        name_to_label = {
            dataset["name"]: dataset["label"]
            for class_ in self.library_metadata.standard_metadata.get("classes", [])
            for dataset in class_.get("datasets", [])
            if dataset.get("label")
        }
        return name_to_label.get(self.params.domain, "")
