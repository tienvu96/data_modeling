import json
import boto3
import time
import urllib.parse 
import datetime
import re 
import time

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    glue_client = boto3.client('glue')
    
    bucket_name ='cdp-trigger-data-model'
    get_last_modified = lambda obj: int(obj.last_modified.strftime('%s'))
    bckt = s3.Bucket("cdp-trigger-data-model")
    objs = [obj for obj in bckt.objects.all()]
    objsort = [obj for obj in sorted(objs, key=get_last_modified)]
    
    # show all files in delta bucket
    list_all_file = []
    for i in objsort:
        list_all_file.append(i.key)
        
    # create delta table name and delta table path
    delta_table_name_list = []
    delta_name = list_all_file[-1].split("/")[2]
    delta_table_name_list.append(delta_name)

    newest_file = list_all_file[-1]
    sub_delta_path = newest_file.rsplit('_delta_log', 1)[0]
    delta_table_name = delta_table_name_list[0]
    
    crawler_name = "DE_" + delta_table_name
    delta_table_path = "s3://"+ bucket_name +"/"+sub_delta_path
    
    print(delta_table_path)
    print(crawler_name)
    
    # check crawler
    all_crawlers = []
    response = glue_client.get_crawlers()
    all_crawlers.extend(response['Crawlers'])
    while 'NextToken' in response:
        next_token = response['NextToken']
        response = glue_client.get_crawlers(NextToken=next_token)
        all_crawlers.extend(response['Crawlers'])
    
    all_crawler_names = []
    for i in all_crawlers:
        all_crawler_names.append(i['Name'])
    
    #########___CREATE__NEW__CRAWLER__#########
    if "_symlink_format_manifest/manifest" not in list_all_file[-1] \
        and ".json" in list_all_file[-1] \
        and "delta_log" in list_all_file[-1] \
        and "snappy.parquet" in list_all_file[-2] and crawler_name not in all_crawler_names:
        print("have to create crawler")
        
        response = glue_client.create_crawler(
        Name=crawler_name,
        Role='arn:aws:iam::321179548224:role/glue_access_redshift_s3', 
        DatabaseName='cdp_data_model_miley',
        Description='',
        Targets={
            'DeltaTargets': [
                {
                    'DeltaTables': [
                        delta_table_path,
                    ],
                    'WriteManifest': True
                },
            ]
        },
        SchemaChangePolicy={
            'UpdateBehavior': 'UPDATE_IN_DATABASE',
            'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
        },
        RecrawlPolicy={
            'RecrawlBehavior': 'CRAWL_EVERYTHING'
        },
        LineageConfiguration={
            'CrawlerLineageSettings': 'DISABLE'
        })
        
        response = glue_client.start_crawler(Name=crawler_name)
        
    
    #########___UPDATE__CRAWLER__#########
    elif ("snappy.parquet" in list_all_file[-1] or ".json" in list_all_file[-1]) and crawler_name in all_crawler_names:
        print("have to update other delta_table")
        
        response = glue_client.get_crawler(Name=crawler_name)
        crawler_status = response['Crawler']['State']
        
        
        if crawler_status == "READY":
            glue_client.update_crawler(
            Name=crawler_name, 
            Role=response['Crawler']['Role'],
            Targets=response['Crawler']['Targets'],
            SchemaChangePolicy={
                'UpdateBehavior': 'UPDATE_IN_DATABASE',
                'DeleteBehavior': 'DEPRECATE_IN_DATABASE' 
            })
            
            response = glue_client.start_crawler(Name=crawler_name)
            
           
            
        else:
            "can't update"
    else:
        "can't create and update"
    
    
    return "success" 