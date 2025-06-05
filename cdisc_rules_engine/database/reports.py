import json
from openpyxl import Workbook
from typing import Dict, List
from cdisc_rules_engine.database.connection import db

class ReportGenerator:
    """ Generate reports from validation results. """

    def generate_excel_report(self, execution_id: str, output_path: str):
        """ Generate an excel report with results and summary. """
        results = self._get_validation_results(execution_id)
        summary = self._get_validation_summary(execution_id)

        wb = Workbook()

        ws_summary = wb.active
        if ws_summary is not None:
            ws_summary.title = "Summary"
            self._write_summary_sheet(ws_summary, summary)

            ws_details = wb.create_sheet("Details")
            self._write_details_sheet(ws_details, results)

            wb.save(f"{output_path}.xlsx")

    def generate_json_report(self, execution_id: str, output_path: str):
        """ Generate a json report with results and summary. """
        results = self._get_validation_results(execution_id)
        summary = self._get_validation_summary(execution_id)

        report = {
            'execution_id': execution_id,
            'summary': summary,
            'results': results
        }

        with open(f"{output_path}.json", 'w') as f:
            json.dump(report, f, indent=2)

    @staticmethod
    def _get_validation_results(execution_id: str) -> List[Dict]:
        """ Get validation results with SQL. """
        query = """
                SELECT
                    r.core_id,
                    d.dataset_name,
                    d.domain,
                    vr.status,
                    vr.row_number,
                    vr.variables,
                    vr.error_message,
                    vr.error_details
                FROM cdisc.validation_results vr
                         JOIN cdisc.rules r ON vr.rule_id = r.rule_id
                         JOIN cdisc.datasets d ON vr.dataset_id = d.dataset_id
                WHERE vr.execution_id = %s
                ORDER BY d.dataset_name, r.core_id, vr.row_number \
                """
        return db.execute_query(query, (execution_id,))

    @staticmethod
    def _get_validation_summary(execution_id: str) -> Dict:
        """ Get validation summary with SQL. """
        query = """
                SELECT
                    COUNT(DISTINCT dataset_id) as total_datasets,
                    COUNT(DISTINCT rule_id) as total_rules,
                    COUNT(CASE WHEN status = 'ERROR' THEN 1 END) as total_errors,
                    COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END) as total_success,
                    COUNT(CASE WHEN status = 'EXECUTION_ERROR' THEN 1 END) as total_failures
                FROM cdisc.validation_results
                WHERE execution_id = %s \
                """
        return db.execute_query(query, (execution_id,))[0]

    @staticmethod
    def _write_summary_sheet(ws, summary: Dict):
        """ Write summary sheet. """
        ws.append(['Validation Summary'])
        ws.append([])
        ws.append(['Total Datasets:', summary['total_datasets']])
        ws.append(['Total Rules:', summary['total_rules']])
        ws.append(['Total Errors:', summary['total_errors']])
        ws.append(['Successful Validations:', summary['total_success']])
        ws.append(['Failed Executions:', summary['total_failures']])

    @staticmethod
    def _write_details_sheet(ws, results: List[Dict]):
        """ Write details sheet. """
        headers = ['Rule ID', 'Dataset', 'Domain', 'Status',
                   'Row', 'Variables', 'Error Message']
        ws.append(headers)

        for result in results:
            if result['status'] == 'ERROR':
                variables_str = ', '.join(result['variables'] or [])
                ws.append([
                    result['core_id'],
                    result['dataset_name'],
                    result['domain'],
                    result['status'],
                    result['row_number'],
                    variables_str,
                    result['error_message']
                ])
