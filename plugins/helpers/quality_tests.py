import logging

class QualityTest:
    
    select_sql = '''
        SELECT count(*)
        FROM {}
    '''
    
    def __init__(self, 
                 sql='', table='',
                 records=None, 
                 validation=None):
        self.sql = sql
        self.table = table
        self.records = records
        self.validation=validation
        
    def validate(self):
        return self.validation(self)

    def has_rows(self):
        if len(self.records) < 1 or len(self.records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
        num_records = self.records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table} contained 0 rows")
        logging.info(f"Data quality on table {self.table} check passed with {num_records} records")
        
    @staticmethod
    def has_rows_test(table):
        quality_test = QualityTest(
            sql = QualityTest.select_sql.format(table),
            validation=QualityTest.has_rows,
            table = table
        )
        return quality_test
            
    
            
        
