import boto3
import requests
from requests_aws4auth import AWS4Auth
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging
import json

# delete index
def delete(index):
    result = es.indices.delete(index, timeout='120s')
    logger.info("delete index: "+ index + str(result))
    while es.indices.exists(index) == True:
        logger.info('wait for deleting '+ index)
        time.sleep(3)

# create new index with new schema
def create(index, docName):
    if es.indices.exists(index) == True:
        delete(index)
    data = schemaTemplate.replace('@docName@', docName)
    result = es.indices.create(index , timeout='120s', body = data)
    logger.info(str(result))
    while es.indices.exists(index) == False:
        logger.info('wait for creating '+ index)
        time.sleep(3)

#reindex index from source to destination async
def reindex(source_index, destination_index):
    result = es.reindex({
         "source": {"index": source_index},
         "dest": {"index": destination_index}
        }, wait_for_completion=False)
    logger.info("reindex index: "+ source_index + str(result))
    return result

#check task status by task id
def checkTask(taskId):
    return es.tasks.get(taskId)

def dedupExtraColumn(subId):
    source = '''
        List columnNames = new ArrayList();
        Iterator it = ctx._source.user_columns.iterator();
        while (it.hasNext()) {
            String column = it.next().column;
            if(columnNames.contains(column)){
                it.remove();
                continue;
            }
            columnNames.add(column);
        }
    '''
    updateScript = {
        "script" : {
                "source": source,
                "lang": "painless"
        },
        "query": {
            "term": {
                "tsc_sub_id": subId
            }
        }
    }
    return es.update_by_query(index='transactions_user_columns', doc_type='extra_props_columns', body=updateScript)


# Define a function for the thread
def reindexThread(source_index):
    try:
        logger.info("Thread start for: " + source_index)
        destination_index = source_index+"_v1"
        docName = source_index[17:] 
        log = "=====ReindexLog=======" + source_index + "\n"
        create(destination_index, docName)
        log +=(destination_index+ " created.\n")
        result = reindex(source_index, destination_index)
        
        taskId = result['task']
        status = checkTask(taskId)
        while status['completed']!=True:
            logger.info(source_index + " reindex wait 10 second")
            time.sleep(10)
            status = checkTask(taskId)
        
        log += (taskId + " first reindex status:" + str(status)[0:256] +"\n")
       
        if not status['response']['failures']:
            retry = 0
            while(retry<=2):
                try:
                    if retry>0:
                        time.sleep(5)
                    delete(source_index)
                    break
                except Exception as e:
                    logger.warn(e)
                    if retry==2:
                        logger.error(source_index + " delete failed")
            
            log += (source_index + " deleted\n")
            create(source_index, docName)
            log +=(source_index+ " created.\n")
            result = reindex(destination_index, source_index)
            taskId = result['task']
            while status['completed']!=True:
                logger.info(destination_index + " reindex wait 10 second")
                time.sleep(10)
                status = checkTask(taskId)
                
            log += (taskId + " second reindex status:" + str(status) +"\n");
            if  not status['response']['failures']:
                while(retry<=2):
                    try:
                        if retry>0:
                            time.sleep(5)
                        delete(destination_index)
                        break
                    except Exception as e:
                        logger.warn(e)
                        if retry==2:
                            logger.error(destination_index + " delete failed")

                log += (destination_index + " deleted\n");
            log+= "Reindex Completed!\n"
        logger.info("Thread finished:" + source_index)
    except Exception as e:
        logger.error(e)
    finally:
        lock.acquire()
        resultLog.write(log)
        lock.release()

def reindex_all():
    #thread pool
    executor = ThreadPoolExecutor(max_workers=3)
    #get all indicies
    indices_state = es.cluster.state()['metadata']['indices']
    logger.info("indices number:" + str(len(indices_state.keys())))
    count = 0
    for source_index in sorted(indices_state.keys(), reverse=True):
        indexFile.write(source_index + "\n")
        if len(source_index) == 43 and source_index.startswith("tcta_transaction_"):
        # if len(source_index) == 43 and source_index != 'tcta_transaction_01d4q59gqp40e4r0jmzzzs27g5' and source_index!= 'tcta_transaction_01ckf60mjaqbt035p6qwqtgw8e':
        
        # #remove _v1 indices
        # if source_index.endswith("_v1"):
        #     delete(source_index)
            count += 1
            logger.info("reindex: " + source_index)

            #Create threads for each index
            # try:
            #     # use ThreadPoolExecutor
            #     executor.submit(reindexThread,source_index,)
            # except Exception as e:
            #     logger.error("Error: unable to start thread for" + source_index)

    logger.info("==============reindex_count=====================")
    logger.info(str(count) + " indicies need to be reindex ")
    logger.info("==============reindex_count=====================")

    executor.shutdown(wait=True)

# set logger
logger = logging.getLogger('reindex')
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')

fileHandler = logging.FileHandler('info.log')
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler) 

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(consoleHandler)

logger.setLevel(logging.INFO)

logger.info("reindex start...")
indexFile = open("indices.txt", "w")
resultLog = open("result.txt", "w")

with open('transaction_schema.json', 'r') as file:
    schemaTemplate = file.read()

lock = Lock()

logger.info("connect es to get all indices...")
# config elasticsearch server
# region = 'us-west-2' # e.g. us-west-1
# service = 'es'
# credentials = boto3.Session().get_credentials()
# awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
# es = Elasticsearch(
#     hosts=[{'host': 'search-tasencrypted2-mghb7faauldlbne6gzopzg4dim.us-west-2.es.amazonaws.com', 'port': 443}],
#     http_auth=awsauth,
#     use_ssl=True,
#     verify_certs=True,
#     timeout=600,
#     connection_class=RequestsHttpConnection)

es = Elasticsearch(
    ['search-taselasticsearchdomain-o4d3cjvkwipif4crdkxfk4knva.us-west-2.es.amazonaws.com'],
    scheme="https",
    port=443,)

test_index='transactions_user_columns'
#subId="01DT293KVTA1D1HH05X3DPC1JE"
subId="12345"

#================================================
# get index
#print(es.indices.get(test_index))

# search index
res = es.search(index=test_index, body={"query": {"match_all": {}}}, size=100)
with open('output.txt', 'w') as outfile:
    json.dump(res, outfile)

# delete index
# delete('tcta_transaction_01ckf60mjaqbt035p6qwqtgw8e')

# create index
# logger.info(create("tcta_transaction_01ckf60mjaqbt035p6qwqtgw8e", "01ckf60mjaqbt035p6qwqtgw8e"))

# # reindex
# result  = reindex('tcta_transaction_01ckf60mjaqbt035p6qwqtgw8e_backup','tcta_transaction_01ckf60mjaqbt035p6qwqtgw8e')

# taskId = result['task']
# status = checkTask(taskId)
# while status['completed']!=True:
#     logger.info(" reindex wait 10 second")
#     time.sleep(10)
#     status = checkTask(taskId)
# check task result
# result = checkTask('2vWXa0jAQvycmT2JwKZcCA:1782534')
# logger.info(result)

# dedup user_columns
print("dedup........")
print(dedupExtraColumn(subId))
#===================================================

# check index exist or not, return True or False
#logger.info(es.indices.exists('abc'))


#reindex_all()

logger.info("all reindex finished")
indexFile.close
resultLog.close