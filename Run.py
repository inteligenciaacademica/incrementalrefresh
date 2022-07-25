import json
import os, io, sys, csv
from pickle import FALSE # utilizado para converter tipo de carecteres  UTF8
import xml.etree.ElementTree as ET
import codecs, time

from datetime import datetime


def abreModelo(fileModelo):
    try:
        path = fileModelo
        print("Abrindo arquivo :" +fileModelo)
        f = open(path, encoding='utf-8')
        
        model = f.read()
        f.close()
        rmodel = json.loads(model)
        return rmodel 
    except:
        print("Problemas ao abrir o arquivo!\n Verifique se o mesmo existe!",)


#Esta função é responsável por aplicar  as regras ao modelo 

def mesclaModelo(Modelo, Parametros): 
    

    #verifica em  Regras quais tabelas serão consideradas com suas respectivas regras
    for pan  in Parametros['Table']:
        Nometabela = pan["name"]
        Regra = {}
        Regra['refreshPolicy'] = pan["regra"]
        Partitions =None # pan['partitions']
        EncontrouTabela = None
        i =0
        #Busca a tabela que irá receber a regra  do modelo 
        for tab in Modelo['model']['tables']:
            if (tab['name'] == Nometabela):
                EncontrouTabela= True 
                break
            i+=1
        if (EncontrouTabela==True):
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



def abreRegra(path):
    path 
    print("Abrindo arquivo :" +fileModelo)
    f = open(path, encoding='utf-8')
    
    text = f.read()
    f.close()



def configuraRegra(Modelo,pathDir):



    #-------- Verifica o arquivo  com os nomes das tabelas 
    if (os.path.exists(pathDir)==False):
        print("Relação de tabelas não encontradas!")
        exit()
    #cria objeto json com a estrutura necessaria
    TabelasRegras ={"Table":[],"expressions":[]}
    linha = 0 
    with open(pathDir , 'r') as csvfile:
        _reader = csv.reader(csvfile, skipinitialspace=True)
        _header = next(_reader)
        _Conf_Header =['Nometabela', 'coluna', 'rollingWindowGranularity', 'rollingWindowPeriods', 'incrementalGranularity', 'incrementalPeriods', 'incrementalPeriodsOffset']
        if(_Conf_Header != _header ):
            print("Arquivo inválido ! verifique a estrutura\n", _Conf_Header)
            sys.exit("Erro no arquivo  TabelasRegra.txt")
        
        for row in _reader:
            if row == []:# Se linha estiver sem o nome da tabela sair 
                break
            Nometabela, column, rollingWindowGranularity, rollingWindowPeriods, incrementalGranularity, incrementalPeriods, incrementalPeriodsOffset = row
            _Nometabela = Nometabela.strip()
            _column = column.strip()
            _rollingWindowGranularity = rollingWindowGranularity.strip()
            _rollingWindowPeriods  = int(rollingWindowPeriods.strip())
            _incrementalGranularity  = incrementalGranularity.strip()
            _incrementalPeriods  = int(incrementalPeriods.strip())
            _incrementalPeriodsOffset = int(incrementalPeriodsOffset.strip())



            print(" tabela:" ,_Nometabela,"coluna:" ,_column)
            EncontrouTabela = None
            EncontrouColuna  = None
            i=0
            if Nometabela == "": # Se linha estiver sem o nome da tabela sair 
                break
          
            for tab in Modelo['model']['tables']: ##For Tabelas
                if (tab['name'] == _Nometabela):
                    EncontrouTabela= True 
                    _TabelaPartition =Modelo['model']['tables'][i]['partitions']
                    Columns =Modelo['model']['tables'][i]['columns']
                    print("Busca coluna:")
                    for col in Columns: #for Colunas
                        if(col['name'] ==_column.strip()):
                            print("Coluna encontrada:" ,_column )
                            EncontrouColuna = True
                            _pollingExpression  = "let MaxPeriodo = List.Max(#\""+_Nometabela+"\"["+ _column +"]), accountForNull = if MaxPeriodo = null then #datetime(1901, 01, 01, 00, 00, 00) else MaxPeriodo in accountForNull"
                            break #se achou a coluna sair dor for Tabelas
                    break #Se achou tabelas sair dor For tabela
                i= i+1
            if EncontrouTabela!=True:
                print("Tabela ",_Nometabela," não encontrada!")
            if EncontrouColuna!=True:
                print("Coluna ", _column ,"não encontrada!\n")

                
            if EncontrouColuna == True and EncontrouColuna == True :
                Regra={}
                Regra['policyType'] ='basic'
                Regra['rollingWindowGranularity'] =_rollingWindowGranularity
                Regra['rollingWindowPeriods'] =_rollingWindowPeriods
                Regra['incrementalGranularity'] =_incrementalGranularity
                Regra['incrementalPeriods'] =_incrementalPeriods
                if _incrementalPeriodsOffset == -1:
                    Regra['incrementalPeriodsOffset'] =_incrementalPeriodsOffset
                    
                Regra['pollingExpression'] =_pollingExpression
                Regra['sourceExpression'] =_TabelaPartition[0]['source']['expression']
            
                #------------------------------------------------
                tabela ={}
                tabela['name'] =Nometabela
                tabela['partitions'] =_TabelaPartition
                tabela['regra']= Regra
                
                TabelasRegras["Table"].append(tabela)
        

    RangeStart = {"name":"RangeStart","kind":"m", "expression":[
                "let",
                "    Source = #datetime(2018, 1, 1, 0, 0, 0) meta [IsParameterQuery=true, Type=\"DateTime\", IsParameterQueryRequired=true]",
                "in",
                "    Source"]}
    RangeEnd = {"name":"RangeEnd","kind":"m", "expression":[
                "let",
                "    Source = #datetime(2023, 12, 31, 0, 0, 0) meta [IsParameterQuery=true, Type=\"DateTime\", IsParameterQueryRequired=true]",
                "in",
                "    Source"]}
    TabelasRegras['expressions']= [RangeStart,RangeEnd]
    #TabelasRegras['annotations']= [{"Name":"geraRegra","value":time.time()}]
    return TabelasRegras
   
#Função Gerana novo modelo, Esta função ira decompor o modelo atual para  gerar um novo  mesclando os arquivo contendo a regras
def GeraNovoModelo(NovaRgera):

    
    RegraName = "Regras.json"
    ModeloName = "Model.bim"
    SubDir ="Tmls"
    TabelasRegra =  "TabelasRegra.txt"
    file_path = os.path.dirname(os.path.realpath(__file__))
    NomeModelo, file_path = GetNameProject(file_path)

    
    fileModelo = file_path+ "\\"+ModeloName
    rmodel = abreModelo(fileModelo)

    #verifica se existe uam regra caso não ele irá gerar novamente 
    if (os.path.exists(file_path+"\\"+RegraName)==False):
        Regra = configuraRegra(rmodel,file_path+ "\\"+TabelasRegra)
        salvaNovoModelo(Regra, file_path,RegraName)
    # para soluncionar pontualmente um problema forço a geração de um novo aquivo de regra a cada execução 
    if( NovaRgera == True):
       Regra = configuraRegra(rmodel,file_path+ "\\"+TabelasRegra)
       salvaNovoModelo(Regra, file_path,RegraName)

    fileRefresh = file_path+"\\"+RegraName


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
    M_discourageImplicitMeasures=rmodel.get('discourageImplicitMeasures')
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
    M['name']= NomeModelo
    M['discourageImplicitMeasures']=True
    M['culture']= rmodel['culture']
    M['dataSources']= rmodel['dataSources']
    M['tables']=rmodel['tables']
    M['relationships']=rmodel['relationships']
    M['roles']=rmodel['roles']
    M['expressions']=M_expressions
    M['annotations']=rmodel['annotations']
    M['annotations'].append({"Name":"geraRegra","value":time.time(),"data":datetime.today().strftime('%Y-%m-%d %H:%M:%S')})
 
    ModelCreate['createOrReplace']['object']['database']  =NomeModelo
    ModelCreate['createOrReplace']['database']['name'] =NomeModelo
    ModelCreate['createOrReplace']['database']['compatibilityLevel'] =1550
    ModelCreate['createOrReplace']['database']['model']  =M
    DataSource =ModelCreate['createOrReplace']['database']['model']['dataSources'][0]['name']
    ModelCreate['createOrReplace']['database']['model']['dataSources'][0]['name'] = New_DataSource 

    #substitui o nome da fonte dados 
    try:
        for tab in ModelCreate['createOrReplace']['database']['model']['tables']:
            tab['partitions'][0]['source']['expression'][1] = tab['partitions'][0]['source']['expression'][1].replace(DataSource,New_DataSource)
    except:
        pass

    refresh_model = {   "refresh": {  "type": "full", "objects": [   {        "database": NomeModelo      }    ]  }}
  
    salvaNovoModelo(ModelCreate,file_path+"\\"+SubDir,ModeloName)
    salvaNovoModelo(refresh_model,file_path+"\\"+SubDir,"refresh_model.tmls")


def main():
    #busca o diretório do arquivo run.py
    Texto = '--------------------------------------------------------------------------\n'
    Texto = Texto+'Esse script deve ser salvo na pasta raiz do projeto.\n'
    Texto = Texto+'-Para informar as tabelas que serão configuradas com Incremental Refresh\n'
    Texto = Texto+' deve ser configurado um arquivo "TabelasRegra.txt" com o nome da tabela e coluda de referência\n'
    Texto = Texto+'\t  Exemplo:\n'
    Texto = Texto+'\t  "FI DF007_PartAbertoClientesForn",   "DataReferencia"\n'
    Texto = Texto+'\t \n'
    Texto = Texto+'\t-O novo modelo será salvo no diretório Tmls como nome Model.bim. \n'
    Texto = Texto+'\t  \n'
    Texto = Texto+'Após a implementação  é necessário atualizar as credências em PowerBi Premium (Data source credentials) \n'
    Texto = Texto+'PowerBi Premium (Data source credentials)  \n'
    Texto = Texto+'--------------------------------------------------------------------------\n'
    print(Texto)
    
   #começa aqui Passo1 
    NovaRgera = True 
    GeraNovoModelo(NovaRgera)
    
    


def pause(massage = 'press any key to continue'):  # this function will pause the script with a default massage or a custome one.
    print(massage)
    os.system('pause >NULL')  # this will pause untill any key is pressed.
    return 0




if __name__ == "__main__":
    
    main()
    #pause()
