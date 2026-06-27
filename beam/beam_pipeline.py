import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import pyarrow as pa
from datetime import datetime

# Definición del esquema estricto para Parquet [cite: 78, 79]
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
        ('batch_id', pa.string())
    ]))
])

class ValidateSalesFn(beam.DoFn):
    def process(self, element):
        try:
            # Separar CSV
            cols = element.split(',')
            if len(cols) != 5:
                yield beam.pvalue.TaggedOutput('rejected', f"{element},INVALID_COLUMNS")
                return
            
            t_id, store, sku, amount_str, currency = cols
            
            # Validación de nulos [cite: 73]
            if not t_id or not sku:
                yield beam.pvalue.TaggedOutput('rejected', f"{element},NULL_CRITICAL_FIELD")
                return
            
            # Validación financiera [cite: 72]
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
    def process(self, element):
        cols = element.split(',')
        if len(cols) != 3:
            return
        
        t_id, status, date_str = cols
        
        # Validación de estados permitidos y nulos [cite: 73]
        valid_statuses = ['CREATED', 'PENDING', 'COMPLETED', 'REFUNDED']
        if not t_id or not status or status not in valid_statuses:
            yield beam.pvalue.TaggedOutput('rejected', f"{element},INVALID_STATUS_OR_NULL")
            return
            
        yield (t_id, {'type': 'log', 'status': status, 'date': date_str})

class BuildNestedRecordFn(beam.DoFn):
    def process(self, element):
        t_id, data = element
        sales = data.get('sales', [])
        logs = data.get('logs', [])
        
        if not sales:
            return
            
        sale = sales[0]
        
        # Parsear fechas y estructurar [cite: 76, 79]
        status_history = []
        for log in logs:
            dt = datetime.strptime(log['date'], "%Y-%m-%dT%H:%M:%SZ")
            status_history.append({'status': log['status'], 'date': dt})
            
        record = {
            'id': t_id,
            'store': sale['store'],
            'financials': {
                'raw_amount': sale['amount'],
                'currency': sale['currency']
            },
            'status_history': status_history,
            'metadata': {
                'processed_at': datetime.utcnow(),
                'batch_id': 'BATCH-001'
            }
        }
        yield record

def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        
        # 1. Ingesta Multifuente [cite: 70]
        sales_raw = p | 'Read Sales' >> beam.io.ReadFromText('data/sales_data.csv', skip_header_lines=1)
        logs_raw = p | 'Read Logs' >> beam.io.ReadFromText('data/status_logs.csv', skip_header_lines=1)
        
        # 2. Validación y Side Outputs [cite: 71]
        sales_validated = sales_raw | 'Validate Sales' >> beam.ParDo(ValidateSalesFn()).with_outputs('rejected', main='valid')
        logs_validated = logs_raw | 'Validate Logs' >> beam.ParDo(ValidateLogsFn()).with_outputs('rejected', main='valid')
        
        # Sinking de Errores a CSV [cite: 74]
        rejected_data = (
            (sales_validated.rejected, logs_validated.rejected)
            | 'Flatten Rejected' >> beam.Flatten()
            | 'Write Rejected' >> beam.io.WriteToText('rejected_sales', file_name_suffix='.csv')
        )
        
        # 3. Join y Denormalización [cite: 75]
        joined = (
            {'sales': sales_validated.valid, 'logs': logs_validated.valid}
            | 'CoGroupByKey' >> beam.CoGroupByKey()
            | 'Build Nested Record' >> beam.ParDo(BuildNestedRecordFn())
        )
        
        # 4. Capa Silver (Parquet) [cite: 77, 78]
        joined | 'Write to Parquet' >> beam.io.WriteToParquet(
            'silver_layer/sales_enriched',
            schema=SILVER_SCHEMA,
            file_name_suffix='.parquet'
        )

if __name__ == '__main__':
    run()