import pandas as pd
from elasticsearch import Elasticsearch
import ast
import json
import numpy as np
from datetime import datetime
import psycopg2
import sqlalchemy as sa
from sqlalchemy import create_engine
import re
import time
from sqlalchemy.orm import sessionmaker
import traceback
import csv
import logging
import requests
import sqlite3
import urllib.parse
import os


# important:  database name postgres or snowflake
working_on='postgres'

# IP of ES
host = ''
esport = 

# db creds for both postgres and snowflake
DATABASE = ""
USER = ""
PASSWORD = urllib.parse.quote_plus("")
HOST = ""
PORT = ""
schema = ""

# only for snowflake else leave empty
ACCOUNT = ""
WAREHOUSE = ""
ROLE = ""


def connection():
    engine = None
    try:
        if working_on=='snowflake':
            connection_string = "snowflake://%s:%s@%s/%s/public?warehouse=%s" % (USER, PASSWORD,ACCOUNT,DATABASE,WAREHOUSE)
            engine = sa.create_engine(connection_string)
        else:
            connection_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (USER, PASSWORD, HOST, str(PORT), DATABASE)
            engine = sa.create_engine(connection_string)
    except Exception as e:
        print("Error During Connection: " + str(e))
    return engine


def conn_to_redshift():
    engine = None
    try:
        if working_on=='snowflake':
            connection_string = "snowflake://%s:%s@%s/%s/public?warehouse=%s" % (USER, PASSWORD,ACCOUNT,DATABASE,WAREHOUSE)
            engine = sa.create_engine(connection_string)
        else:
            connection_string = "postgresql+psycopg2://%s:%s@%s:%s/%s" % (USER, PASSWORD, HOST, str(PORT), DATABASE)

            engine = sa.create_engine(connection_string)
        conn = engine.connect()
        trans = conn.begin()
    except Exception as e:
        print("Error During Connection: " + str(e))
    return conn, trans



def epoch_to_datetime(item):
    check = re.compile(r'^[0-9]+$').match(str(item).split('.')[0])
    if check:
        return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(eval(str(item)) / 1000))
    else:
        return item


def es_to_redshift(ind):
    try:
        logging.info("-------------Starting-------------")
        print('----------------making connection to ES--------------------------------')
        logging.info("----------------making connection to ES--------------------------------")
        es = Elasticsearch([{'host': host, 'port': esport, 'timeout': 60}])

        rstype = None
        if (ind == 'pd_care_management_v4'):
            rstype = [
                # 'adczdchoc_tasks', 'UU', 'noFFtes',
                # 'care_plans_inDSterventions', 'caSDre_plans', 'healthcoach', 'goalUUs',
                # 'patient_interventionsSDG_linked_goals', 'HJ', 'care_EEERplans_risk_factors',
                # 'raw_care_protocol_engaHGgements'] 
        else:
            rstype = ['asse334ssments']

        for tp in rstype:
            connect = connection()
            if tp.lower() in ['timeline', 'adhoc_tasks', 'health_module_reassignment_history', 'notes',
                              'care_plans_interventions', 'care_plans', 'healthcoach', 'goals',
                              'patient_interventions_linked_goals',
                              'care_plans_linked_goals', 'care_plans_risk_factors', 'raw_care_protocol_engagements',
                              "goals", 'patient_interventions_linked_goals',
                              'care_plans_engagements', 'care_plans_engagements', 'care_plans',
                              'care_plans_linked_assessments','adhoc_tasks',
                              'timer', 'patient_goal_barrier_mapping', 'raw_care_protocol_tasks',
                              'raw_care_protocol_encounters', 'patient_resource_history',
                              'resources', 'resource_categories', 'resource_sub_categories',
                              'patient_resource_categories']:
                prefix = 'pd_'
            else:
                # tp in ['health_modules','health_module_tasks','task_encounter_questions','health_module_units','task_encounter_responses']
                prefix = 'pd_care_'
            print(tp)
            logging.info(tp)
            table_name = prefix + tp + "_test_"
            print(table_name)
            # implementSql(table_name, schema)

            connect.execute("DROP TABLE IF EXISTS " +schema+"."+ table_name + " CASCADE")
            print("----------------- DROPPED table "+ table_name+ " and creating a new one-------------------")
            logging.info("----------------- DROPPED table "+ table_name+ " and creating a new one-------------------")

            Query = ''
            if (tp == 'task_encounter_responses'):
                Query = '''
		        {
					"from": 0,
					"query": {
						"bool": {
							"must": {
								"match": {
									"resource_type": {
										"query": "task_encounter_responses",
										"type": "phrase"
									}
								}
							}
						}
					},
					"_source": {
						"includes": [
							"control_type",
							"date_response",
							"display_string",
							"hmt_id",
							"key",
							"qid",
							"resource_type",
							"task_id",
							"response"
						],
						"excludes": []
					}
				}'''

            else:
                Query = '''
				{
				"from": 0,
				"query": {
					"bool": {
					"must": []
					}
				},
				"_source": {
					"includes": [],
					"excludes": []
				} }'''

            index_of_es = ind
            doctype = tp

            res = es.search(index=index_of_es, body=Query, doc_type=str(tp), scroll='2m', size="10000")
            logging.info("------------- Starting to run on table "+ table_name+"  -------------")
            print('---------------------converting dict to dataframe--------------------')
            print("Got %d Hits:" % res['hits']['total'])

            scroll_size = res['hits']['total']
            sid = res['_scroll_id']

            if (res['hits']['total'] == 0): continue

            while (scroll_size > 0):
                pandas_df = pd.DataFrame.from_dict(res['hits']['hits'])
                df_hits = pandas_df[['_source']]
                logging.info(df_hits)

                df_x = pd.DataFrame(df_hits['_source'].to_list())
                logging.info("-------------Fetched data from ES-------------")
                print('---------------fill rate of each columns------------------')
                percent_missing = df_x.isnull().sum() * 100 / len(df_x)

                missing_value_df = pd.DataFrame({'column_name': df_x.columns,
                                                 'percent_missing': percent_missing})
                print(missing_value_df)
                logging.info(missing_value_df)
                df_x.columns = [x.lower() for x in df_x.columns]

                datetime_cols = ['added_on', 'assigned_on', 'completed_on', "updated_on",'created_on', 'scheduled_at',"updated_on","scheduledon","scheduled_on",""
                                 'scheduled_time',"edited_on","updatedon","createdon","completedon","addedon","assignedon","createdon","added_on","created_on","editedon","endedon"
                                 'sent_date', 'date_response',"dob","scheduled_at","due_date","deleted_on","closed_on","re_opened_on","date_response","send_date","dob"]

                for col in datetime_cols:
                    if col in list(df_x.columns) :
                        df_x[col] = df_x[col].apply(lambda x: x.replace('T', ' ').split('.')[0] if type(x) == str else x)
                        df_x[col] = df_x[col].apply(lambda x: x.replace(',', ' ').split('.')[0] if type(x) == str else x)
                        df_x[col] = df_x[col].apply(lambda x: epoch_to_datetime(x))
                        df_x[col] = pd.to_datetime(df_x[col])

                if tp=='task_encounter_responses':
                    df_x['response']=df_x['response'].str.lower()

                df_x['ingestion_datetime']= pd.to_datetime('now')

                print('---------------------basic ststistics-----------------')
                print('---------------------connection with DB-----------------')
                logging.info("------------transfering dataframe to postgres--------------")

                print('----------------transfering dataframe to postgres--------------------------')
                transfer_start = datetime.now()
                print(transfer_start)
                logging.info("-------------insert to postgres-------------")

                df_x.to_sql(table_name, connect, schema=schema, method='multi', if_exists='append',
                            index=False, chunksize=100000)  # make chunksize : 50000

                total_time_taken = (datetime.now() - transfer_start).seconds / 60
                print("timetaken in minutes is :", total_time_taken)

                print("------------transfer successful----------------------")
                res = es.scroll(scroll_id=sid, scroll='2m')
                sid = res['_scroll_id']
                scroll_size = len(res['hits']['hits'])

                if (scroll_size == 0): continue
            print(f'----------------------------------------------------EXECUTION OF {prefix+tp} DONE WITH TOTAL HITS: {res["hits"]["total"]}-------------------------------------------------------')
            logging.info(f'---------------------------------------------------EXECUTION OF {prefix+tp} DONE WITH TOTAL HITS: {res["hits"]["total"]}------------------------------------------------------')

    except:
        print("error while transferring data from ES to DB")
        message = traceback.format_exc()
        print(message)
        logging.error(message)

        with open('error.csv', 'w') as file:
            writer = csv.writer(file)
            writer.writerow(message)


# not used anymore
def implementSql(table_name, schema):
    try:
        logging.info("-------------implementSql-------------")
        print("starting transfer to DB")
        conn, trans = conn_to_redshift()
        script_start = datetime.now()
        # files = os.listdir('pd_care.sql')
        dim_start_time = datetime.now()
        # print('Running scripts for -', files)
        # logging.info("-------------calling create_pat_details-------------")
        sql_script = create_pat_details(table_name, schema)
        # logging.info("-------------insert to l3.pd_care_patient_details-------------")

        conn.execute(sql_script)
        dim_time_taken = (datetime.now() - dim_start_time).seconds
        print('Time taken -', dim_time_taken, 'seconds')
        dims_finished = datetime.now()
        dims_time_taken = (dims_finished - script_start).seconds / 60
        print('Time taken for dimensions-', dims_time_taken, 'minutes\n')
        trans.commit()
        dim_status = True
    except:
        print("error in implementSql")
        message = traceback.format_exc()
        print(message)
        logging.error(message)


# not in use
def create_pat_details(table_name, schema):
    sql = 'drop table if exists ' + str(schema) + '.' + str(table_name) +' CASCADE'

    return sql


if __name__ == "__main__":
    try:
        os.remove("es_def_care.log")
        os.remove("error.csv")
    except OSError:
        pass
    logging.basicConfig(filename='es_def_care.log', filemode='a',
                        format='%(asctime)s - %(levelname)s ======> %(message)s', level=logging.INFO)
    logging.info("------------- Removing es_def_care.log -------------")
    # implementSql()
    # code for care protocols and care plans
    es_to_redshift('pd_care_management_v4')
    # call for  assessment
    es_to_redshift('pd_care_assessment')
    logging.info("------------------------------------------------------------------------------EXECUTION OF ES TO DB DONE---------------------------------------------------------------------------")
    print("-------------------------------------------------------------------------------EXECUTION OF ES TO DB DONE---------------------------------------------------------------------------")
# implementSql()
incare.py
Displaying incare.py.
