# -*- coding: utf-8 -*-
"""
Created on Wed Sep 15 13:41:48 2021

@author: maria
"""

# Libraries import
import requests
import pandas as pd
from pandas.io.json import json_normalize 
#from sqlalchemy import create_engine 
import time
from time import localtime,strftime 
import getpass
import psycopg2


# Database configuration
def db_config():
    
    df_config = pd.read_csv('intelli_config.csv', header=0)
    username = df_config['username'][0]
    password = df_config['password'][0]
    port = df_config['port'][0]
    
    return username,password,port

# Get eof terms and synonyms
def get_terms_syns():
    
    response_1 = requests.get("http://www.ebi.ac.uk/ols/api/ontologies/efo/terms")
    resp_json_1 = response_1.json()
    
    resp_df = json_normalize(data=resp_json_1['_embedded']['terms'], record_path=['synonyms'],meta=['iri', 'short_form','label'])
    resp_df = resp_df.rename(columns={0: 'synonyms_label','label': 'efo_label'})
    resp_df['synonym_id'] = resp_df.synonyms_label.apply(hash)
    
    # dataframe for terms synonyms mapping, like intermediate keys table
    syn_keys_df = resp_df[["short_form", "synonym_id"]].drop_duplicates()
    
    # dataframe for unique synonyms 
    synonyms_df = resp_df[["synonym_id", "synonyms_label"]].drop_duplicates()
    
    # dataframe for unique eof terms
    terms_df = resp_df[["short_form", "efo_label","iri"]].drop_duplicates()
    
    return syn_keys_df,synonyms_df,terms_df

# Function to retrieve parent links and then parent terms and ontology name
def get_parent(iri):
    
    response = requests.get("http://www.ebi.ac.uk/ols/api/ontologies/efo/terms/"+iri) 
    resp_json = response.json()
    
    links_df = json_normalize(data=resp_json)
    
    links_df = links_df[['iri', 'label','short_form','_links.parents.href']]
    
    parent = requests.get(links_df['_links.parents.href'][0]) 
    parent_json = parent.json()
    parent_df = json_normalize(data=parent_json['_embedded']['terms'])
    parent_df = parent_df[['ontology_name', 'short_form']]
    parent_df['parent_href']= links_df['_links.parents.href'][0]
    parent_df = parent_df.rename(columns={'ontology_name': 'parent_ontology_name', 'short_form': 'parent_short_form'})
    links_unified = links_df.merge(parent_df,left_on='_links.parents.href', right_on='parent_href', how='left')
    
    return links_unified

# Function to write the tables in postgres
def write_to_postgre(username,password,port,syn_keys_df,synonyms_df,terms_df,link_all_df):
    
    ##### create logic insert into select not in 
    connection = psycopg2.connect(user=username,
                                  password=str(password),
                                  port=str(port),
                                  database="postgres")
    
    cursor = connection.cursor()
    
    # Synonyms insert for values that do not exist in table by using conflict
    synonyms_df_columns = list(synonyms_df)
    columns1 = ','.join(synonyms_df_columns)
    values_list1 = list(synonyms_df.itertuples(index=False, name=None))
    values1 = str(values_list1).replace("[","").replace("]","")
    update_list1 = ["{} = EXCLUDED.{} ".format(col, col) for col in synonyms_df]
    update_str1 = ','.join(update_list1)
    
    insert_stmt1 = 'INSERT INTO {} ({}) VALUES {} ON CONFLICT (synonym_id) DO UPDATE SET {}'.format("synonyms_df", columns1, values1, update_str1)
    cursor.execute(insert_stmt1)
    
    # Terms insert for values that do not exist in table by using conflict
    terms_df_columns = list(terms_df)
    columns3 = ','.join(terms_df_columns)
    values_list3 = list(terms_df.itertuples(index=False, name=None))
    values3 = str(values_list3).replace("[","").replace("]","")
    update_list3 = ["{} = EXCLUDED.{} ".format(col, col) for col in terms_df]
    update_str3 = ','.join(update_list3)
    
    insert_stmt3 = 'INSERT INTO {} ({}) VALUES {} ON CONFLICT (short_form) DO UPDATE SET {}'.format("terms_df", columns3, values3, update_str3)
    cursor.execute(insert_stmt3)
    
    
    # Terms and syn kyes table insert for values that do not exist in table by using conflict
    syn_keys_df['concatvalue'] = syn_keys_df['short_form'].map(str) + syn_keys_df['synonym_id'].map(str)
    
    syn_keys_columns = list(syn_keys_df)
    columns2 = ','.join(syn_keys_columns)
    values_list2 = list(syn_keys_df.itertuples(index=False, name=None))
    values2 = str(values_list2).replace("[","").replace("]","")
    update_list2 = ["{} = EXCLUDED.{} ".format(col, col) for col in syn_keys_columns]
    update_str2 = ','.join(update_list2)
    
    insert_stmt2 = 'INSERT INTO {} ({}) VALUES {} ON CONFLICT (concatvalue) DO UPDATE SET {}'.format("syn_keys_df", columns2, values2, update_str2)
    cursor.execute(insert_stmt2)
    
    cursor.close()
    #engine = create_engine('postgresql://'+username+':'+str(password)+'@localhost:5432/postgres')
    #syn_keys_df.to_sql('syn_keys_df', schema = 'public', con =engine, if_exists = 'replace',index = False)
    #synonyms_df.to_sql('synonyms_df', schema = 'public', con =engine, if_exists = 'replace',index = False)
    #terms_df.to_sql('terms_df', schema = 'public', con =engine, if_exists = 'replace',index = False)
    #link_all_df.to_sql('link_all_df', schema = 'public', con =engine, if_exists = 'replace',index = False)
    #engine.dispose()
    
if __name__ == "__main__":
    
    #applying try catch for error handling and export in txt file (otherwise if we have s3 bucket we could keep there the logs
    # or create a postgres logs table)
    try:
        
        #################### For logs Purpose ##################
        t0=time.time()
        
        #Current system user that runs the script
        CurrentUser = getpass.getuser()
        
        #Start time of script
        StartTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
        
        #################### For logs Purpose ##################
        
        # Postgres Username/Password
        username,password,port = db_config()
        
        # return terms-synonyms dataframes
        syn_keys_df,synonyms_df,terms_df = get_terms_syns()
        
        # get list of unique iris in order to loop and extract parent hrefs content
        iri_list = terms_df['iri'].str.replace('://', '%253A%252F%252F').str.replace('/', '%252F').unique()
        
        appended_data = []
        
        # loop through iris and concat the results into one single dataframe
        for iri in iri_list:
        
            links_unified = get_parent(iri)
        
            appended_data.append(links_unified)
        
        
        link_all_df = pd.concat(appended_data)
        link_all_df = link_all_df.rename(columns ={'short_form':'child_short_form'}).drop(['iri','label','_links.parents.href'], 1).drop_duplicates()
        
        #write results to the db
        write_to_postgre(username,password,port,syn_keys_df,synonyms_df,terms_df,link_all_df)
        
        #################### For logs Purpose ##################
        
        #script end time  
        EndTime = strftime("%Y-%m-%d %H:%M:%S", localtime())
        #write success logs in txt
        
        #################### For logs Purpose ##################
        
        with open("Success_logs.txt", "w") as text_file:
            file = "Current User: "+CurrentUser+", Start Time: "+StartTime+", End Time: "+EndTime+", Success"
            text_file.write(file)
    #error handling, in case an error occurs a txt file is written in S3 logs dir with a failure message
  
    except Exception as err:
    
        s = str(err).replace("'","`")
    
        End = strftime("%Y-%m-%d %H:%M:%S", localtime())
         
        file = "Current User: "+CurrentUser+", Start Time: "+StartTime+", End Time: "+End+", Success"+", Failure: "+s
    
        # Export logs txt in case of failure
        with open("Failure_logs.txt", "w") as text_file:
            text_file.write(file)


