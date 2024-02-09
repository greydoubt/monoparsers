# columnar -> graph parser
# github: greydoubt
# ontology / graph statements
# parquet converter and importer

citation = "# Parquet to Graph Triples Monoparser, available from https://github.com/greydoubt/monoparsers"

# how to use:
# python3 monoparser.py input_file.pqt output_file.ttl

import sys
import pandas as pd 

def router(df, lim: int) -> None:
    total = 0 
    for index, row in df.iterrows():
    
        if row['buildID'] == None:
            ...
    
        else:
            if row['unit_count'] < 1.0:
                ...
            
            #elif row['unit_count'] == 1.0: 
            # 
            elif len(row['units'])  <= 1:
                print("mono found")
                new_feature = monoProc(total)   
                writeFeature(new_feature)
        
            #elif row['unit_count'] > 1.0: 
            elif len(row['units'])  > 1:
                print("poly")
                new_feature = polyProc(total)   
                writeFeature(new_feature)
    
            else:
                ...
        
            total += 1
    
            if total == lim:
                break 

def loadParquet(pqt_file: str) -> None:
    return pd.read_parquet(pqt_file, engine='pyarrow')

def exportCSV(target_df, csv_file = r'parquet_output.csv') -> None:
    target_df.to_csv(csv_file, index = False) 

def writeFeature(sometext: str, filename = sys.argv[2]) -> None:
    with open(filename, 'a+') as f:
        f.write(sometext) 
        f.seek(0)
        f.write('\n')

def sourceHarness(sometext: str) -> None:

    if sometext:
        return "ufokn:fromDataSource " + ",".join(
            (
                sometext.replace(
                "oa", "ufokn:OAdataSource",1).replace(
                "ms", "ufokn:MSdataSource",1).replace(
                "osm", "ufokn:OSMdataSource",1)
                ).split("-")
            ) + ";"

def monoProc(temp_pointer: int) -> str:

    ftAtRisk1_1 = '''### http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer]['buildID']) + '''
    <http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer]['buildID']) + '''> rdf:type owl:NamedIndividual ,
                                                                              ufokn:FeatureAtRisk ;
                                                                         ufokn:hasAddress <http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer]['buildID']) + '''> ;
                                                                         ufokn:hasRiskPoint <http://schema.ufokn.org/ufokn-core/v2/RiskPoint/''' + str(df.loc[temp_pointer]['buildID']) + '''> ;
                                                                         ufokn:ms_id "''' + str(df.loc[temp_pointer]['ms_id']) + '''" ;
                                                                         ufokn:key "''' + str(df.loc[temp_pointer]['KEY']) + '''" ;
                                                                         ufokn:oa_id "''' + str(df.loc[temp_pointer]['oa_id'][0]) + '''" ;
                                                                         ''' + sourceHarness(df.loc[temp_pointer].loc['source']) + ''' 
                                                                         ufokn:value "''' + str(df.loc[temp_pointer]['VALUE']) + '''" .''' + '\n'             
    
    
    ftAtRisk1_2 = '''###  http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer]['buildID']) + '''
    <http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer]['buildID'])+'''> rdf:type owl:NamedIndividual ,
                                                                              ufokn:StreetAddress ;
                                                                     ufokn:street "''' + str(df.loc[temp_pointer]['street']) + '''" ;
                                                                     ufokn:streetNumber "''' + str(df.loc[temp_pointer]['number'][0]) + '''";
                                                                     ufokn:city "''' + str(df.loc[temp_pointer]['city']) + '''" ; 
                                                                     ufokn:region "" ; 
                                                                     ufokn:state "";
                                                                     ufokn:postcode "''' + str(df.loc[temp_pointer]['postcode']) + '''".''' + '\n'
    

    ftAtRisk1_3 = '''###  http://schema.ufokn.org/ufokn-core/v2/RiskPoint/''' + str(df.loc[temp_pointer]['buildID']) + '''
    <http://schema.ufokn.org/ufokn-core/v2/RiskPoint/''' + str(df.loc[temp_pointer]['buildID']) + '''> rdf:type owl:NamedIndividual ,
                                                                                ufokn:RiskPoint ;
                                                                       <http://www.opengis.net/spec/geosparql/1.0#hasGeometry> "''' + str(df.loc[temp_pointer]['geometry']) + '''"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .'''+'\n'
    
    
    return ftAtRisk1_1 + '\n' + ftAtRisk1_2 + '\n' + ftAtRisk1_3 + '\n'


    
def polyProc(temp_pointer) -> str:

    subLink2 = map( lambda subunitnumber: 
               "<http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/" + 
               str(df.loc[temp_pointer].loc['buildID']) +
               "/" +
               str(subunitnumber+1) + ">" , 
               range(int(len(df.loc[temp_pointer].loc['units'])) )
               )



    subUnit = map( lambda subunitnumber:
'''
###  http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer].loc['buildID']) + '''/''' + str(subunitnumber+1) + '''
<http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer].loc['buildID']) + '''/''' + str(subunitnumber+1) + '''>rdf:type owl:NamedIndividual ,
                                                                              ufokn:FeatureAtRisk ;
                                                                     ufokn:hasAddress <http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer].loc['buildID']) + '''/''' + str(subunitnumber+1)  + '''> ;''' +
                                                                     '''ufokn:hasRiskPoint <http://schema.ufokn.org/ufokn-core/v2/RiskPoint/''' + str(df.loc[temp_pointer].loc['buildID']) + '''> ; 
                                                                     ufokn:ms_id "''' + str(df.loc[temp_pointer]['ms_id']) + '''" ;
                                                                         ufokn:key "''' + str(df.loc[temp_pointer]['KEY']) + '''" ;
                                                                         ufokn:oa_id "''' + str(df.loc[temp_pointer]['oa_id'][0]) + '''" ;
                                                                         ''' + sourceHarness(df.loc[temp_pointer].loc['source']) + ''' 
                                                                         ufokn:value "''' + str(df.loc[temp_pointer]['VALUE']) + '''" .''' + '\n' +                                                                                                                        




'''
###  http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer].loc['buildID']) + '''/'''+ str(subunitnumber+1) +'''
<http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer].loc['buildID']) + '''/'''+ str(subunitnumber+1) +'''> rdf:type owl:NamedIndividual ,
                                                                                      ufokn:UnitAddress ;  
                                                                             ufokn:hasStreetAddress <http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer].loc['buildID']) + '''> ; 
                                                                             ufokn:unitNumber "''' + df.loc[temp_pointer].loc['units'][subunitnumber] + '''".''',
                                                                             range(int(len(df.loc[temp_pointer].loc['units'])) 

                                                                                ) 
                                                                                )



    ftAtRisk2_1 = '''
    ###  http://schema.ufokn.org/ufokn-core/v2/CollectionOfFeatures/''' + str(df.loc[temp_pointer]['buildID']) + '''
    <http://schema.ufokn.org/ufokn-core/v2/CollectionOfFeatures/''' + str(df.loc[temp_pointer]['buildID']) + '''> rdf:type owl:NamedIndividual ,
                                                                                                ufokn:CollectionOfFeatures ;''' + "ufokn:hasMember " + ",\n".join(subLink2) + ";" + ''' 
                                                                                       
                                                                                       ufokn:hasAddress <http://schema.ufokn.org/ufokn-core/v2/Address/UFOKN-dq0dk3bp96> .'''
                                                                                      
    

    ftAtRisk2_2 = '''
                ###  http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer]['buildID']) + '''
                <http://schema.ufokn.org/ufokn-core/v2/Address/''' + str(df.loc[temp_pointer]['buildID']) + '''> rdf:type owl:NamedIndividual ,
                                                                                                      ufokn:StreetAddress ; 
                                                                                             ufokn:street "''' + str(df.loc[temp_pointer]['street']) + '''" ;
                                                                                            ufokn:streetNumber "''' + str(df.loc[temp_pointer]['number'][0]) + '''";
                                                                                            ufokn:city "''' + str(df.loc[temp_pointer]['city']) + '''" ; 

                                                                                             ufokn:postcode "''' + str(df.loc[temp_pointer]['postcode']) + '''".'''
    
                              
    
    ftAtRisk2_3 = '''    
                ###  http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/RiskPoint/UFOKN-dq0dk3bp96
                <http://schema.ufokn.org/ufokn-core/v2/RiskPoint/UFOKN-dq0dk3bp96> rdf:type owl:NamedIndividual ,
                                                                                                        ufokn:RiskPoint ;
                                                                                               <http://www.opengis.net/spec/geosparql/1.0#hasGeometry> "''' + str(df.loc[temp_pointer]['geometry']) + '''"^^<http://www.opengis.net/ont/geosparql#wktLiteral> .'''+'\n'
                                            
    ftAtRisk2_4 = '''                           
                ###  http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer]['buildID']) + '''
                <http://schema.ufokn.org/ufokn-core/v2/FeatureAtRisk/''' + str(df.loc[temp_pointer]['buildID']) + '''> rdf:type owl:NamedIndividual .'''
    

    return ftAtRisk2_1 + '\n' + ftAtRisk2_2 + '\n' + ftAtRisk2_3 + '\n' + "\n".join(subUnit) + '\n' 



if __name__ == '__main__':


    header = '''
    @prefix : <http://schema.ufokn.org/ufokn-core/v2/> .
    @prefix ufokn: <http://schema.ufokn.org/core/v2/> .
    @prefix ufokn-geo: <http://schema.ufokn.org/geo/v1/> .
    @prefix geo: <http://www.opengis.net/spec/geosparql/1.0/> .
    @prefix owl: <http://www.w3.org/2002/07/owl#> .
    @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
    @prefix xml: <http://www.w3.org/XML/1998/namespace> .
    @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
    @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
    @prefix sosa: <http://www.w3.org/ns/sosa/> .
    @prefix time: <http://www.w3.org/2006/time#> .
    @prefix dcterms: <http://purl.org/dc/terms/> .
    @base <http://schema.ufokn.org/example/> .

    
    '''

    writeFeature(header)

    writeFeature(citation)

    parquet_file = sys.argv[1]

    df = loadParquet(parquet_file) 

    router(df, 90000) 
