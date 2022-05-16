import json
import os, io, sys
from pickle import FALSE
import xml.etree.ElementTree as ET
import codecs


def abreModelo(fileModelo):

    path = fileModelo
    print("Abre arquivo :" +fileModelo)
    f = open(path, encoding='utf-8')
    
    model = f.read()
    f.close()
    rmodel = json.loads(model)
    return rmodel 


#Esta função é responsável por aplicar  as regras ao modelo 

def mesclaModelo(Modelo, Parametros): 
    

    #verifica em  Regras quais tabelas serão consideradas com suas respectivas regras
    for pan  in Parametros['Table']:
        Nometabela = pan["name"]
        Regra = {}
        Regra['refreshPolicy'] = pan["regra"]
        Partitions =None # pan['partitions']

        i =0
        #Busca a tabela que irá receber a regra  do modelo 
        for tab in Modelo['model']['tables']:
            if (tab['name'] == Nometabela):
                f= True 
                break
            i+=1
        if (f==True):
            #aplica regra 
            
            part = Modelo['model']['tables'][i]
            objTabela ={"name":"","refreshPolicy":"","columns":"","partitions":""}
            for  N   in part:
                if  N == 'name':
                    objTabela['name'] = part['name']
                    objTabela['refreshPolicy'] =pan["regra"]
                      
                if  N == 'columns':
                    objTabela['columns'] = part['columns']
                if  N == 'measures':
                    objTabela['measures'] = part['measures']
                if  N == 'isHidden':
                    objTabela['isHidden'] = part['isHidden']
                if  N == 'partitions':
                    objTabela['partitions'] = part['partitions']
                else:
                 objTabela[N] = tab[N]
            


            Modelo['model']['tables'][i] =objTabela
            if(Partitions):
                part = Modelo['model']['tables'][i]['partitions']
                Modelo['model']['tables'][i]['partitions']=Partitions

            
        else:
            print("Tabela não encontrada!")
    return Modelo
    


def salvaNovoModelo(modelo, path,name):
    #verifica se existe a pasta TMLS
    if not os.path.exists(path):
        os.mkdir(path)

    path = path+"\\"+name
    
    with io.open(path, 'w',encoding='utf8') as json_file:
        data = json.dumps(modelo, ensure_ascii=False, indent=4)
        
        
        try:
            json_file.write(data)
        except TypeError:
            # Decode data to Unicode first
         
            json_file.write(data.decode('utf8'))     
    print("Salvo em:" , path )


def GetSource(Modelo):
    #obtem nome da fonte de dados
    Source = Modelo['model']['dataSources'][0]["name"]
    return Source 



#Função para buscar o nome do modelo dentro do arquivo de projeto do Visual Studio
#Será considerado o nome cadastrado em DeploymentServerName  no campo Database
# 
def GetNameProject(path):
  
    #procura o arquivo  SMPRJ e abre o primeiro arquivo que encontrar
    for p, _, files in os.walk(os.path.abspath(path)):
        for file in files:
            if file.lower().endswith(".smproj"):
                smproj = os.path.join(p, file)
                path_smproj =  os.path.join(p)
            
                break


    tree = ET.parse(smproj)
    root = tree.getroot()
    Name = None
    #busca a propriedade  DeploymentServerDatabase e retona seu valor
    for elem in root[1]:
        if elem.tag =='{http://schemas.microsoft.com/developer/msbuild/2003}DeploymentServerDatabase':   
            Name = elem.text
            break
    return Name, path_smproj

def main():
    #busca o diretório do arquivo run.py
    Texto = '--------------------------------------------------------------------------\n'
    Texto = Texto+'Esse script deve ser salvo na pasta raiz do projeto.\n'
    Texto = Texto+'\t-Todas as regras devem ser cadastradas em um arquivo regras.tmls.\n'
    Texto = Texto+'\t-O novo modelo será salvo no diretório Tmls como nome Model.bim. \n'
    Texto = Texto+'\t-Este script pode não funcionar corretamente para casos em que   \n'
    Texto = Texto+'\t utiliza mais de uma fonte de dados \n'
    Texto = Texto+'\t  \n'
    Texto = Texto+'Após a implementação  é necessário atualizar as credências em PowerBi Premium (Data source credentials) \n'
    Texto = Texto+'PowerBi Premium (Data source credentials)  \n'
    Texto = Texto+'--------------------------------------------------------------------------\n'
    print(Texto)


    file_path = os.path.dirname(os.path.realpath(__file__))
    NomeModelo, file_path = GetNameProject(file_path)


    fileModelo = file_path+ "\\Model.bim"
    fileRefresh = file_path+"\\regras.tmls"
    rmodel = abreModelo(fileModelo)
    refresh = abreModelo(fileRefresh)
    rmodel= mesclaModelo(rmodel,refresh)
   
    
    #Define  novo nome da fonte de dados
    New_DataSource=rmodel['model']['dataSources'][0]['name']#"SQL_tdprdsql002"
    
    #Cria um modelo novo , com as instruções  Create or Replace 
    ModelCreate ='{ "createOrReplace": {      "object": {  "database": ""   },  "database":  {} } }'
    ModelCreate= json.loads(ModelCreate) 

    #Obtem O objeto modelo no arquivo original 
    rmodel = rmodel['model']
    
    #Obtem atributos individuais do modelo original , esses valores serão utilizado no Novo Modelo 
    M_culture = rmodel['culture']
    M_discourageImplicitMeasures=rmodel['discourageImplicitMeasures']
    M_dataSources =rmodel['dataSources']
    M_tables=rmodel['tables']
    M_relationships = rmodel['relationships']
    M_roles= rmodel['roles']

    ##Deve ser verificado se exite expressoes, 
    M_expressions= refresh['expressions']
    M_annotations= rmodel['annotations']

    M ='{  }'
    M =  json.loads(M) 
    #Monta Modelo Novo  , com os atributos do modelo original 
    M['defaultPowerBIDataSourceVersion']='powerBI_V3'
    M['discourageImplicitMeasures']=True
    M['culture']= rmodel['culture']
    M['dataSources']= rmodel['dataSources']
    M['tables']=rmodel['tables']
    M['relationships']=rmodel['relationships']
    M['roles']=rmodel['roles']
    M['expressions']=M_expressions
    M['annotations']=rmodel['annotations']
 
    ModelCreate['createOrReplace']['object']['database']  =NomeModelo
    ModelCreate['createOrReplace']['database']['name'] =NomeModelo
    ModelCreate['createOrReplace']['database']['compatibilityLevel'] =1500
    ModelCreate['createOrReplace']['database']['model']  =M
    DataSource =ModelCreate['createOrReplace']['database']['model']['dataSources'][0]['name']
    ModelCreate['createOrReplace']['database']['model']['dataSources'][0]['name'] = New_DataSource 

    #substitui o nome da fonte dados 
    try:
        for tab in ModelCreate['createOrReplace']['database']['model']['tables']:
            tab['partitions'][0]['source']['expression'][1] = tab['partitions'][0]['source']['expression'][1].replace(DataSource,New_DataSource)
    except:
        pass
  
    salvaNovoModelo(ModelCreate,file_path+"\\Tmls","Model.bim")


def pause(massage = 'press any key to continue'):  # this function will pause the script with a default massage or a custome one.
    print(massage)
    os.system('pause >NULL')  # this will pause untill any key is pressed.
    return 0
  


if __name__ == "__main__":
    
    main()
    #pause()