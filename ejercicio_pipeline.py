import pandas as pd
from bamboo_lib.models import Parameter, EasyPipeline, PipelineStep
from bamboo_lib.steps import DownloadStep, LoadStep
from bamboo_lib.connectors.models import Connector

#DownloadStep
#TransformStep
#LoadStep

class TransformStep(PipelineStep):
    def run_step(self, prev, params):  #Se que puedo usar params para obteenr parametros, pero en este paso que real utilidad le puedo dar?

        df = pd.read_csv(prev)    
        #Aplico los mismo pasos de panda x ahora
        df=df.melt(id_vars=['puma_id','occupation_id'],value_vars=['men_20','men_30','women_20','women_30']) #Tranformo al formato tidy
        df['sex_id'] = df['variable'].apply(lambda x: '1' if x == 'men_20' or x=='men_30' else '2') #Separo por genero
        df['age_id'] = df['variable'].apply(lambda x: '1' if x == 'men_20' or x=='women_20' else '2') #Separo por edad
        #Selecciono solo los valores que necesito
        df=df[['puma_id','occupation_id','sex_id','age_id','value']]
        df.columns=['puma_id','occupation_id','sex_id','age_id','total'] #Renombro columnas
        print(df.head())
        #print(df.info())   #Me srive para revisar si hay nulos
        df=df.copy() #Copio el valor para guardarlo y luego lo retorno
        df['year']=params.get('year')
        print(params.get('year'))
        return df

class ExamplePipeline(EasyPipeline):
    @staticmethod
    def parameter_list(): #¿Para qué?
        return [Parameter(label='year',name='year',dtype='int')]

    @staticmethod
    def steps(params):
        download_step = DownloadStep(connector="sample-data",connector_path="conns.yaml") #Descargo el archivo ebtregandoi el nombre del conector del conns, y entregando la ruta que lo almacena
    
        transform_step = TransformStep()
        db_conector=Connector.fetch('clickhouse-local',open('conns.yaml'))
        dtype={'puma_id':'String',
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