"""
Apache Beam Pipeline for Global Mart Data Consolidation.
"""
import os
import threading
from queue import Queue
from typing import Dict, Any, List, Tuple, Iterable
from datetime import datetime, timezone
from dotenv import load_dotenv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa

load_dotenv()

SILVER_SCHEMA = pa.schema([
    ('id', pa.string()),
    ('store', pa.string()),
    ('financials', pa.struct([
        ('raw_amount', pa.float64()),
        ('currency', pa.string())
    ])),
    ('status_history', pa.list_(pa.struct([
        ('status', pa.string()),
        ('date', pa.timestamp('us'))
    ]))),
    ('metadata', pa.struct([
        ('processed_at', pa.timestamp('us')),
        ('batch_id', pa.string()),
        ('is_active', pa.bool_())
    ]))
])

class ParallelCSVReaderFn(beam.DoFn):
    """
    Reads a CSV file in parallel using Python threading and locks.
    """

    def process(self, file_path: str) -> Iterable[str]:
        """
        Processes a file path, reads it using threads, and yields lines.

        Args:
            file_path (str): The absolute path to the CSV file.

        Yields:
            str: A single line from the CSV file.
        """
        file_queue: Queue = Queue()
        results: Queue = Queue()
        lock = threading.Lock()

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                next(f) 
                for line in f:
                    file_queue.put(line.strip())
        except FileNotFoundError:
            return

        def worker() -> None:
            """Worker thread function to process lines."""
            while not file_queue.empty():
                with lock:
                    if file_queue.empty():
                        break
                    line = file_queue.get()
                results.put(line)

        threads: List[threading.Thread] = []
        for _ in range(4):
            t = threading.Thread(target=worker)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()

        while not results.empty():
            yield results.get()

class ValidateSalesFn(beam.DoFn):
    """
    Validates sales data records.
    """
    
    def process(self, element: str) -> Iterable[Any]:
        """
        Parses and validates a single CSV line of sales data.

        Args:
            element (str): CSV line.

        Yields:
            Any: Validated tuple or tagged rejection.
        """
        try:
            cols = element.split(',')
            if len(cols) != 5:
                yield beam.pvalue.TaggedOutput('rejected', f"{element},INVALID_COLUMNS")
                return
            
            t_id, store, sku, amount_str, currency = cols
            
            if not t_id or not sku:
                yield beam.pvalue.TaggedOutput('rejected', f"{element},NULL_CRITICAL_FIELD")
                return
            
            try:
                amount = float(amount_str)
                if amount <= 0:
                    yield beam.pvalue.TaggedOutput('rejected', f"{element},NEGATIVE_OR_ZERO_AMOUNT")
                    return
            except ValueError:
                yield beam.pvalue.TaggedOutput('rejected', f"{element},NON_NUMERIC_AMOUNT")
                return
                
            yield (t_id, {'type': 'sale', 'store': store, 'amount': amount, 'currency': currency})
        except Exception as e:
            yield beam.pvalue.TaggedOutput('rejected', f"{element},ERROR:{str(e)}")

class ValidateLogsFn(beam.DoFn):
    """
    Validates status logs data records.
    """

    def process(self, element: str) -> Iterable[Any]:
        """
        Parses and validates a single CSV line of status logs.

        Args:
            element (str): CSV line.

        Yields:
            Any: Validated tuple or tagged rejection.
        """
        cols = element.split(',')
        if len(cols) != 3:
            return
        
        t_id, status, date_str = cols
        valid_statuses = ['CREATED', 'PENDING', 'COMPLETED', 'REFUNDED']
        
        if not t_id or not status or status not in valid_statuses:
            yield beam.pvalue.TaggedOutput('rejected', f"{element},INVALID_STATUS_OR_NULL")
            return
            
        yield (t_id, {'type': 'log', 'status': status, 'date': date_str})

class BuildNestedRecordFn(beam.DoFn):
    """
    Combines validated sales and logs into a nested record.
    """

    def process(self, element: Tuple[str, Dict[str, List[Dict[str, Any]]]]) -> Iterable[Dict[str, Any]]:
        """
        Builds the final nested dictionary representing a transaction.

        Args:
            element (Tuple[str, Dict[str, List[Dict[str, Any]]]]): Grouped data by transaction ID.

        Yields:
            Dict[str, Any]: A nested record dictionary.
        """
        t_id, data = element
        sales = data.get('sales', [])
        logs = data.get('logs', [])
        
        if not sales:
            return
            
        sale = sales[0]
        status_history = []
        is_refunded = False
        is_completed = False

        for log in logs:
            try:
                dt = datetime.strptime(log['date'], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            
            status_history.append({'status': log['status'], 'date': dt})
            if log['status'] == 'REFUNDED':
                is_refunded = True
            if log['status'] == 'COMPLETED':
                is_completed = True
            
        is_active = is_completed and not is_refunded

        record: Dict[str, Any] = {
            'id': t_id,
            'store': sale['store'],
            'financials': {
                'raw_amount': sale['amount'],
                'currency': sale['currency']
            },
            'status_history': status_history,
            'metadata': {
                'processed_at': datetime.now(timezone.utc),
                'batch_id': 'BATCH-002',
                'is_active': is_active
            }
        }
        yield record

def run_pipeline() -> None:
    """
    Configures and runs the Apache Beam pipeline.
    """
    options = PipelineOptions()
    project_root = os.getenv('PROJECT_ROOT', '/tmp')
    silver_layer = os.getenv('SILVER_LAYER_PATH', f"{project_root}/silver_layer")
    rejected_path = os.path.join(project_root, 'rejected_sales')
    sales_file = os.path.join(project_root, 'data', 'sales_data.csv')
    logs_file = os.path.join(project_root, 'data', 'status_logs.csv')

    with beam.Pipeline(options=options) as p:
        
        sales_raw = (
            p 
            | 'Create Sales File Path' >> beam.Create([sales_file])
            | 'Read Sales Parallel' >> beam.ParDo(ParallelCSVReaderFn())
        )
        logs_raw = (
            p 
            | 'Create Logs File Path' >> beam.Create([logs_file])
            | 'Read Logs Parallel' >> beam.ParDo(ParallelCSVReaderFn())
        )
        
        sales_validated = sales_raw | 'Validate Sales' >> beam.ParDo(ValidateSalesFn()).with_outputs('rejected', main='valid')
        logs_validated = logs_raw | 'Validate Logs' >> beam.ParDo(ValidateLogsFn()).with_outputs('rejected', main='valid')
        
        rejected_data = (
            (sales_validated.rejected, logs_validated.rejected)
            | 'Flatten Rejected' >> beam.Flatten()
            | 'Write Rejected' >> beam.io.WriteToText(rejected_path, file_name_suffix='.csv')
        )
        
        joined = (
            {'sales': sales_validated.valid, 'logs': logs_validated.valid}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Build Nested Record' >> beam.ParDo(BuildNestedRecordFn())
        )
        
        joined | 'Write to Parquet' >> beam.io.WriteToParquet(
            os.path.join(silver_layer, 'sales_enriched'),
            schema=SILVER_SCHEMA,
            file_name_suffix='.parquet'
        )

if __name__ == '__main__':
    run_pipeline()