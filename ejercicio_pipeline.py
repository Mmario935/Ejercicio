import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector

#DownloadStep
#TransformStep
#LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):  #params create a new column with values depend what values i give, for example i gave years to create tables for every year

        df = pd.read_csv(prev)    
        #I can try in ipynb previusly to check if everything is right, and finally copy paste here
        df=df.melt(id_vars=['puma_id','occupation_id'],value_vars=['men_20','men_30','women_20','women_30']) 
        df['sex_id'] = df['variable'].apply(lambda x: '1' if x == 'men_20' or x=='men_30' else '2') 
        df['age_id'] = df['variable'].apply(lambda x: '1' if x == 'men_20' or x=='women_20' else '2') 
        df=df[['puma_id','occupation_id','sex_id','age_id','value']]
        df.columns=['puma_id','occupation_id','sex_id','age_id','total']
        print(df.head())
        #print(df.info())   #Info about dataset, so I can identify the null values
        df=df.copy() 
        df['year']=params.get('year')  #Not necessary at all, only if a want use new parameters wuth parameters_list function
        print(params.get('year'))
        return df

class ExamplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list(): #Create the new parameters column, depending the values i gave.
        return [Parameter(label='year',name='year',dtype='int')]  #Here I use python syntax for dtype

    @staticmethod
    def steps(params):
        download_step = DownloadStep(connector="sample-data",connector_path="conns.yaml") #Download the dataframe from I indicate to the code
    
        transform_step = TransformStep()
        db_conector=Connector.fetch('clickhouse-local',open('conns.yaml'))
        dtype={'puma_id':'String',  #I need to use the Clickhouse Dtype syntax
               'occupation-id':'String',
               'sex_id':'UInt8',
               'age_id':'UInt8',
               'total':'Int16',
               'year':'UInt16'}
        load_step=LoadStep('puma_population',db_conector,if_exists='append',dtype=dtype,pk=['puma_id','occupation_id','sex_id','age_id','total','year'],nullable_list=[]) #borrar drop, happen agregar datos   

        return [download_step, transform_step,load_step]
    
#Ejecuto todo 
if __name__ == "__main__":
    example_pipeline = ExamplePipeline()
    for year in [2023,2024,2025]:
        example_pipeline.run({'year':year})