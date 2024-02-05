import logging
from pymongo import MongoClient
import datetime

import pandas as pd 
def Fetching_Query(db,mongo_uri,BQ_DATASET,BQ_TABLE,batch_size=10000):
   
    client=MongoClient(mongo_uri)
    logging.info('Successfully connected to MongoDB')
    
    try:
        
        logging.info("Fetching distinct MSISDN count.")

        MSISDN_Notification = client[db]['mtn_sa_notification_master'].aggregate([
                      {
                        "$match": {
                          "Group_Name": "mtnsar3",
                        },
                      },
                      {
                        "$group": {
                          "_id": "$MSISDN",
                        },
                      },
                      {
                        "$count":
                          "Count",
                      },
                    ],allowDiskUse=True) 

        total_records = next(MSISDN_Notification)['Count']
        
        logging.info(f'Total distinct MSISDN records: {total_records}')
        
        logging.info("Current Time")
        current_date = datetime.datetime.now().strftime('%Y-%m-%d')

        logging.info("Checking if a document for the current date already exists")
        existing_log = client[db]['mtn_sa_bigquery_logs'].find_one({'_id': current_date})

        if not existing_log:
            logging.info("Inserting a new document")
            log_document = {
                'CURRENT_DATE': current_date,
                'STATUS': 'INPROGRESS',
                'INDICATOR': "UNIQUE_MSISDN",
                'PROCESS_START_TIME': datetime.datetime.now(),
                'MSISDN_COUNT': 0  # Initialize with 0, will be updated later
            }
         
        
            client[db]['mtn_sa_bigquery_logs'].insert_one(log_document)
        
        logging.info("Updating MSISDN_COUNT in mtn_sa_bigquery_logs")
        client[db]['mtn_sa_bigquery_logs'].update_one(
            {'CURRENT_DATE': current_date},
            {'$set': {'MSISDN_COUNT': total_records}}
        )

        skip=0
        
        
        # Processing records in batches
        
        
        while(skip<=total_records):
            batch_query =[
                        {
                            '$match': {
                                'Group_Name': {
                                    '$in': [
                                        'mtnsar3'
                                    ]
                                }
                            }
                        }, {
                            '$sort': {
                                'MSISDN': 1
                            }
                        }, {
                            '$skip': 0
                        },
                       {
                            '$limit':10000
                        },{
                            '$lookup': {
                                'from': 'MTN_DEVICE_TOKEN', 
                                'localField': 'MSISDN', 
                                'foreignField': 'MSISDN', 
                                'as': 'result'
                            }
                        }, {
                        '$project': {
                            '_id': 1, 
                            'Active_Users': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$active_segment', 'active'
                                        ]
                                    }, [
                                        'Active'
                                    ], [
                                        '0'
                                    ]
                                    ]
                                }, 
                                'InActive_Users': {
                                    '$cond': [
                                        {
                                            '$ne': [
                                                '$active_segment', 'active'
                                            ]
                                        }, [
                                            'IN_Active'
                                        ], [
                                            '0'
                                        ]
                                        ]
                                    }, 
                                    'Churned_Users': {
                                        '$cond': [
                                            {
                                                '$eq': [
                                                    '$CHURN', 'true'
                                                ]
                                            }, [
                                                'Churned'
                                            ], [
                                                '0'
                                            ]
                                        ]
                                    }, 
                                'New_Purchasers': {
                                    '$cond': [
                                        {
                                            '$eq': [
                                                '$firstpurchaser_segment', 'firstpurchaser'
                                            ]
                                        }, [
                                            'New_Purchaser'
                                        ], [
                                            '0'
                                        ]
                                    ]
                                }, 
                                'New_Visiters': {
                                    '$cond': [
                                        {
                                            '$eq': [
                                        '$newuser_segment', 'newuser'
                                    ]
                                }, [
                                    'First_Visited'
                                ], [
                                    '0'
                                ]
                            ]
                        }, 
                        'Prepaid_Users': {
                            '$cond': [
                                {
                                    '$eq': [
                                        '$Subscriber_Type', 'Prepaid'
                                    ]
                                }, [
                                        'Prepaid_Users'
                                    ], [
                                        '0'
                                    ]
                                ]
                            }, 
                            'Hybrid_Users': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$Subscriber_Type', 'Hybrid'
                                        ]
                                    }, [
                                        'Hybrid_Users'
                                    ], [
                                        '0'
                                    ]
                                ]
                            }, 
                            'Converged_Users': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$Subscriber_Type', 'Converged'
                                        ]
                                    }, [
                                        'Converged_Users'
                                    ], [
                                        '0'
                                    ]
                                ]
                            }, 
                            'Not_Available_Users': {
                                '$cond': [
                                    {
                                        '$eq': [
                                            '$Subscriber_Type', 'Not-available'
                                        ]
                                    }, [
                                        'Not_Available_Users'
                                    ], [
                                        '0'
                                    ]
                                ]
                            }, 
                            'Returning_Purchasers': {
                                '$cond': [
                            {
                                '$eq': [
                                    '$repurhase_after_days', 1
                                ]
                            }, [
                                'Returning_Purchaser'
                            ], [
                                '0'
                            ]
                        ]
                    }, 
                    'Returning_Users': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$revisit_after_days', 1
                                ]
                            }, [
                                'Returning_Purchaser'
                            ], [
                                '0'
                            ]
                        ]
                    }, 
                    'Postpaid_Users': {
                        '$cond': [
                            {
                                '$eq': [
                                    '$Subscriber_Type', 'Postpaid'
                                        ]
                                    }, [
                                        'Postpaid_Users'
                                    ], [
                                        '0'
                                    ]
                                ]
                            }, 
                            'result': 1
                        }
                    }, {
                        '$project': {
                            '_id': 1, 
                            'Segments': {
                                '$concatArrays': [
                                    '$Active_Users', '$InActive_Users', '$Churned_Users', '$New_Purchasers', '$New_Visiters', '$Prepaid_Users', '$Hybrid_Users', '$Converged_Users', '$Not_Available_Users', '$Returning_Purchasers', '$Returning_Users', '$Postpaid_Users'
                                ]
                            }, 
                            'result': 1
                        }
                    }, {
                        '$project': {
                            '_id': 1, 
                            'Group_Name': '$_id.Group_Name', 
                            'MSISDN': '$_id.MSISDN', 
                            'Segments': {
                                '$filter': {
                                    'input': '$Segments', 
                                    'as': 'element', 
                                'cond': {
                                    '$ne': [
                                        '$$element', '0'
                                    ]
                                }
                            }
                        }, 
                        'result': 1
                    }
                }, {
                    '$project': {
                        '_id': 0, 
                        'MSISDN': '$_id.MSISDN', 
                        'Group_Name': '$_id.Group_Name', 
                        'Segments': 1, 
                        'IN_APP_DEVICE': {
                                    '$arrayElemAt': [
                                        '$result.DEVICETOKEN', 0
                                    ]
                                }
                            }
                        }
                    ]



       
             
            result=client[db]['mtn_sa_notification_master'].aggregate(batch_query,allowDiskUse=True)
            batch_data=list(result)

            # Checking if batch_data has records

            if len(batch_data)>0:
        
                logging.info('Batch IN-APP data loading into the BigQuery')
   
                BigQueryMain(batch_data,BQ_DATASET, BQ_TABLE, mongo_uri, db)
        
            else:
                logging.info('No records remaining. Exiting loop.')
                break
                
            skip+=batch_size
                    
        
        return 'Successfull'
        
    except Exception as e:
        logging.error(f'An Error Occurred: {e}')
        
    finally:
        client.close()
        logging.info('MongoDB connection closed.')
from google.cloud import bigquery
from google.oauth2 import service_account
import datetime
from pymongo import MongoClient


# Initialize BigQuery client
def initialize_bq_client(key_json):
    credentials = service_account.Credentials.from_service_account_info(key_json)
    bq_client = bigquery.Client(credentials=credentials)
    return bq_client


# Initialize MongoDB client
def initialize_mongo_client(CONN_STRING, DATABASE):
    client = MongoClient(CONN_STRING)
    db = client[DATABASE]
    collection = db['big_query_check']
    return collection


# Check if a document with the current date exists in MongoDB
def check_mongo_document(collection):
    current_date = datetime.datetime.now().date()
    current_date_str = current_date.strftime("%Y-%m-%d")
    document = collection.find_one({'_id': current_date_str})
    return document is not None


# Truncate BigQuery table
def truncate_bigquery_table(bq_client, dataset, table):
    truncate_query = f"DELETE FROM `{dataset}.{table}` WHERE segment_labels IS NOT NULL"
    truncate_job = bq_client.query(truncate_query)
    truncate_job.result()


# Insert data into BigQuery
def insert_in_bigquery(bq_client, dataset, table, data, batch_size=500):
    values = []
    update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for index, item in enumerate(data, start=1):
        IN_APP_DEVICE_TOKEN = item.get("IN_APP_DEVICE")
        segment_labels = item.get("Segments")

        values.append(f"('{IN_APP_DEVICE_TOKEN}', {segment_labels}, '{update_time}')")

        if len(values) == batch_size or index == len(data):
            values_str = ', '.join(values)
            query = f"INSERT INTO `{dataset}.{table}` (instance_id, segment_labels, update_time) VALUES {values_str}"
            query_job = bq_client.query(query)
            query_job.result()

            if query_job.error_result:
                print(f"Error occurred while inserting rows: {query_job.error_result}")
                return 'Error'
            values = []

    return 'Success'
def insert_failed_records(collection, data, error):
    try:
        failed_record = {
            "data": data,
            "error_message": error,
            "failed_time": datetime.datetime.now()
        }
        collection.insert_one(failed_record)
    except Exception as e:
        logging.error(f"Error inserting failed record into MongoDB: {str(e)}")


# Main function
def BigQueryMain(data, BQ_DATASET, BQ_TABLE, CONN_STRING, DATABASE):
    # Service account key JSON for BigQuery
    key_json = {
        "type": "service_account",
        "project_id": "mymtn-sa",
        "private_key_id": "0ad8c1c790a02da107d44d64b7299b7d22494f91",
        "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQC6jtajZLaYaQl4\nyScbLdCL+hjO7FBosvM/i+JrT1UKtevwW8KqpcxEZmz8/pW27bXFWD1H8QX0941K\nqtfRsljJ5NZ20F8trpiqdZNRJ7AZl7qwfrdxg9Vuu0xRh3cFnM8V/+wrV49kiWWn\nudRGCrTS0GqVBESwmC4HdqGG9dfNRdqHiBjWreuQGbnXIm7lWHxyf1YCpao6nFmg\ngwf5UJ/HGgeq58XRwuev6bMIZubhltZnel7M45jmmVPZt37D1U28ZTmbeTjcMTfV\n3+ai3Qx+PUtSwIXGDAVRgxlsqAdCZ8Eivc5pscDV8Z2YN3nnqZecNPLiTgt8oyYT\nx1Rzy3rzAgMBAAECggEAKcXDDMoH0r+uNcQAxCaxjC4/cNHcPV1YdzGozLNyTNgo\nUOUTBSfjwasXm1ycBF5ctagI7LtsxInLstznP1aKaAab+PW2YiG2oHB8QI888LYX\nQN7Wz42f9E/vClBlV7XhbsEjzh6ohm/3eaN0AwzxqUSuoNhYUxx4Lgajq3cPz7AD\nENryJrxH6AxvqlaU3jgg0XLvtMUdXxHhePAq+OO5nDbas1BZ6fDzOxvhjHxbCa4Q\nZpwNoo3NU8MpSUVycpj/pTf7FwPerxNvrXs5HlavbC+u3Cyhr/wXOsLBerOHxoe1\nwNJIoMC9sdonfbs3dxCAPNLT41GDa4rY//evUzoGoQKBgQDhbCyNoT3JfzMBsvpP\nIjGXIYJIi6fB30LUancVdxsEAjkrxRLwIwhsdAB62BHk8ugkXFT6xdeLoz/YiiBh\niCRZhNbceHhB82Vy07hyCwvqCLJVMJZNIu49rxZPaOQhkht0bl6+pRz9Kdkkmvn6\nbdG+01UvjgVq9rAQz7p2cAt9kwKBgQDT3RYucwVW8gsbN3SgY7NPzzWArZ/rRFgA\ne3Vy6cl5ZywDtq0Qd8UGJju9y59XXGt6RKM6RT/Ha4RyzH56S0DYwSK9I00Q2fKv\neUWZW5c4ei5wnDQCqxe4+6qRNH+Qjg2mHF7TtA3Vy9TC/m59TJLI3Dg907tF3j5v\n3u87PlVpIQKBgFA1ZFj0sX570xNcsrHrkcebtbbIcmWKYkYgp8Ssf6FahSss0UM+\nw+WLFQygyyUyxSUC8X3VXY+jA7mx1Dm/7mcn3CfQecHFsCg+a1ew1IlulL25LxG9\nRxYNsZuJz/qd+UThbLbbG0h9VnUu65mO7929ZocoOodHxXgF4ev4jC4fAoGAP/t0\nx3JVGnzefcmxnv59GI0rS0EkGpj5OqwOPDX+cnuF/1kbyu1gwwqo4BiudmOi0boI\n0YA4UrFVvpWjXKt9Wfh51UTj8ULg1714F2hhstyzSa7ixiuFbogSaue/3pgH5zKK\nMMUfqIF2L61HEPAfJCndkk5vMBp+IKri9LFOegECgYAQI6PPgbmhI9GscAjFTgBL\nlwbIi5DiWb11o+/ZFr3Qy0+fmfkfDHreA9hYpavutvXVPV9pqR0nIziEscKo40pJ\nNFJCwfKbb6cL73wIAVTkXPURPMRz3r+hjp2+4CErIWDbCjlLPTIh5nfwFE1TNcp+\nWd+QO7bFswvIb8g9bdWaZw==\n-----END PRIVATE KEY-----\n",
        "client_email": "bdb-222@mymtn-sa.iam.gserviceaccount.com",
        "client_id": "116289355914665214578",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/bdb-222%40mymtn-sa.iam.gserviceaccount.com"
    }

    mongo_uri = CONN_STRING

    # BigQuery dataset and table information
    bq_dataset = BQ_DATASET
    bq_table = BQ_TABLE

    bq_client = initialize_bq_client(key_json)
    client = MongoClient(mongo_uri)
    db = client[DATABASE]
    collection = initialize_mongo_client(CONN_STRING, DATABASE)

    try:

        if not check_mongo_document(collection):
            # Truncate the BigQuery table
            truncate_bigquery_table(bq_client, bq_dataset, bq_table)
            insert_result = insert_in_bigquery(bq_client, bq_dataset, bq_table, data)
            current_date = datetime.datetime.now().strftime('%Y-%m-%d')
            process_end_time = datetime.datetime.now()
            result = db['mtn_sa_bigquery_logs'].update_one(
                {"CURRENT_DATE": current_date},
                {
                    '$set': {
                        "PROCESS_END_TIME": process_end_time,
                        "STATUS": "COMPLETED"
                    }
                },
                upsert=True
            )

            if insert_result == 'Success':
                current_date = datetime.datetime.now().date()
                current_date_str = current_date.strftime("%Y-%m-%d")
                collection.insert_one({'_id': current_date_str, 'Category': 'In-App'})

            else:
                logging.info('Failed Event')
                insert_failed_records_into_mongo(db['mongo_bigquery_failure'], data, "Insertion Failed")

        else:
            insert_in_bigquery(bq_client, bq_dataset, bq_table, data)



    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        insert_failed_records_into_mongo(db['mongo_bigquery_failure'], data, str(e))
        logging.info('Failed Event')


    finally:
        client.close()
        bq_client.close()
import pandas as pd 
def starter(db,mongo_uri,BQ_DATASET,BQ_TABLE):
    
    Status=Fetching_Query(db,mongo_uri,BQ_DATASET,BQ_TABLE)
    logging.info(f'BigQuery Job Status is  {Status}')
    