import boto3
import requests
from requests_aws4auth import AWS4Auth
from threading import Thread,Lock
from concurrent.futures import ThreadPoolExecutor
import time
from elasticsearch import Elasticsearch, RequestsHttpConnection
import logging

# delete index
def delete(index):
    result = es.indices.delete(index, timeout='120s')
    logger.info("delete index: "+ index + str(result))
    while es.indices.exists(index) == True:
        logger.info('wait for deleting '+ index)
        time.sleep(2)

# create new index with new schema
def create(index, docName):
    if es.indices.exists(index) == True:
        delete(index)
    data = schemaTemplate.replace('@docName@', docName)
    result = es.indices.create(index , timeout='120s', body = data)
    logger.info(str(result))
    while es.indices.exists(index) == False:
        logger.info('wait for creating '+ index)
        time.sleep(2)

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
            logger.info(source_index + " reindex wait 5 second")
            time.sleep(5)
            status = checkTask(taskId)
        
        log += (taskId + " first reindex status:" + str(status)[0:256] +"\n")
       
        if not status['response']['failures']: 
            delete(source_index)
            log += (source_index + " deleted\n")
            create(source_index, docName)
            log +=(source_index+ " created.\n")
            result = reindex(destination_index, source_index)
            taskId = result['task']
            while status['completed']!=True:
                logger.info(destination_index + " reindex wait 5 second")
                time.sleep(5)
                status = checkTask(taskId)
                
            log += (taskId + " second reindex status:" + str(status) +"\n");
            if  not status['response']['failures']:
                delete(destination_index)
                log += (destination_index + " deleted\n");
            log+= "Reindex Completed!\n"
        logger.info("Thread finished:" + source_index)
    except Exception as e:
        logger.error(e)
    finally:
        lock.acquire()
        resultLog.write(log)
        lock.release()

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


count = 0
lock = Lock()

logger.info("connect es to get all indices...")
# config elasticsearch server
region = 'us-west-2' # e.g. us-west-1
service = 'es'
credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)
es = Elasticsearch(
    hosts=[{'host': 'search-taselasticsearchdomain-l32fm4zxjtiqokree6av7hpj7m.us-west-2.es.amazonaws.com', 'port': 443}],
    http_auth=awsauth,
    use_ssl=True,
    verify_certs=True,
    timeout=600,
    connection_class=RequestsHttpConnection)

# es = Elasticsearch(
#     ['search-taselasticsearchdomain-o4d3cjvkwipif4crdkxfk4knva.us-west-2.es.amazonaws.com'],
#     scheme="https",
#     port=443,)

#================================================
# delete index
# delete('tcta_transaction_01d4q59gqp40e4r0jmzzzs27g5_backup1')

#create index
# logger.info(create("tcta_transaction_01d4q59gqp40e4r0jmzzzs27g5_test", "01ckf60mjaqbt035p6qwqtgw8e"))

# # reindex
# result  = reindex('tcta_transaction_01d4q59gqp40e4r0jmzzzs27g5','tcta_transaction_01d4q59gqp40e4r0jmzzzs27g5_backup1')
# logger.info(result)

# check task result
# result = checkTask('2vWXa0jAQvycmT2JwKZcCA:1782534')
# logger.info(result)
#===================================================

# check index exist or not, return True or False
#logger.info(es.indices.exists('abc'))

#get all indicies
# indices_state = es.cluster.state()['metadata']['indices']
# logger.info("indices number:" + str(len(indices_state.keys())))

#thread pool
executor = ThreadPoolExecutor(max_workers=10)

source_indexs=['']

for source_index in source_indexs:
    logger.info("reindex: " + source_index)
    #Create threads for each index
    try:
        # use ThreadPoolExecutor
        executor.submit(reindexThread,source_index,)
    except Exception as e:
        logger.error("Error: unable to start thread for" + source_index)


logger.info("==============reindex_count=====================")
logger.info(str(len(source_indexs)) + " indicies need to be reindex ")
logger.info("==============reindex_count=====================")

executor.shutdown(wait=True)

logger.info("all reindex finished")
indexFile.close
resultLog.close