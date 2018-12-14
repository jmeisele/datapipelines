import luigi
import pandas as pd
from sqlalchemy import create_engine

class BufferDataIngestion(luigi.Task):

    def run(self):
        engine = create_engine("mssql+pymssql://qlik:qlik138@S138BWXIN012:1433/RB_167_iP_KPI")
        connection = engine.connect()
        query = "SELECT TOP 10 * FROM [RB_167_iP_KPI].[dbo].[Buffer] ORDER BY date desc"
        df = pd.read_sql(sql = query, con = connection)
        df.to_csv(self.output())

    def output(self):
        return luigi.LocalTarget("C:\\USERS\\EISELJA\\DESKTOP\\buffer.csv")

class BufferShiftsDataIngestion(luigi.Task):

    def run(self):
        engine = create_engine("mssql+pymssql://qlik:qlik138@S138BWXIN012:1433/RB_167_iP_KPI")
        connection = engine.connect()
        query = "SELECT TOP 10 * FROM [RB_167_iP_KPI].[dbo].[buffer_shifts] ORDER BY production_day DESC"
        df = pd.read_sql(sql = query, con = connection)
        df.to_csv(self.output())

    def output(self):
        return luigi.LocalTarget("C:\\USERS\\EISELJA\\DESKTOP\\buffer_shifts.csv")

class CombineData(luigi.Task):

    def requires(self):
        yield BufferDataIngestion()
        yield BufferShiftsDataIngestion()

    def run(self):
        df1 = pd.read_csv(BufferDataIngestion().output())
        df2 = pd.read_csv(BufferShiftsDataIngestion().output())
        df = pd.merge(df1, df2, on = "id_buffer")
        df.to_csv(self.output())

    def output(self):
        return luigi.LocalTarget("C:\\USERS\\EISELJA\\DESKTOP\\combined_data.csv")

if __name__ == "__main__":
    luigi.run()
