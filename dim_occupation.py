#Funtions i'll import
import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector

#DownloadStep
#TransformStep
#LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):  
        df = pd.read_csv(prev)  
        df=df[['occupation_id','occupation_name']] #I select the variable I've needed
        df=df.drop_duplicates() #delete duplicated values
        df=df.reset_index(drop=True)
        print(df.head())
        return df

class ExamplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list(): 
        return []

    @staticmethod
    def steps(params):
        download_step = DownloadStep(connector="sample-data",connector_path="conns.yaml") #Descargo el archivo ebtregandoi el nombre del conector del conns, y entregando la ruta que lo almacena
    
        transform_step = TransformStep()
        db_conector=Connector.fetch('clickhouse-local',open('conns.yaml')) 
        dtype={'occupationid':'String',
               'occupation_name':'String'}
        load_step=LoadStep('dim_occupation',db_conector,if_exists='drop',dtype=dtype,pk=['occupation_id'],nullable_list=[]) #borrar drop, append agregar datos

        return [download_step, transform_step,load_step]
    
#Ejecuto todo 
if __name__ == "__main__":
    example_pipeline = ExamplePipeline()
    example_pipeline.run({})