import boto3
import datetime
import time
from create_and_update_crawler import list_files_in_folder


def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    glue_client = boto3.client('glue')
    
    bucket_name = "cdp-trigger-data-model"
    first_subfolder_name = "delta"
    # folder_customer_name = event["folder_name"]
    folder_customer_name ="tientesteightth1272023"
    database_name = folder_customer_name
    
    path_sub_folder = 'delta'+'/'+ folder_customer_name
    
    # call hàm
    files = list_files_in_folder(bucket_name, path_sub_folder)
    
    # delta table path
    filtered_file = [f"s3://{bucket_name}/" +file.replace("_delta_log/00000000000000000000.json","") for file in files if file.endswith("_delta_log/00000000000000000000.json")]
    delta_table_paths = filtered_file
    print("delta paths:",delta_table_paths)

    # crawler name
    clean_crawler_name = ["DE_crawler_default_"  + file.replace("_delta_log/00000000000000000000.json","").replace("/","_")[:-1] for file in files if file.endswith("_delta_log/00000000000000000000.json")]
    crawler_names = clean_crawler_name
    print("crawler names:",crawler_names)
    
    # check if crawler is exist or not?
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
    
    # default_crawler_name, default_delta_table_path
    for crawler_name, delta_table_path in zip(crawler_names, delta_table_paths):
        if crawler_name not in all_crawler_names:
            response = glue_client.create_crawler(
                Name=crawler_name,
                Role='arn:aws:iam::321179548224:role/glue_access_redshift_s3',
                DatabaseName=database_name,
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
                }
            )
    
            response = glue_client.start_crawler(Name=crawler_name)
            print(f"Create name: {crawler_name} successfully")
    
        #########___UPDATE__CRAWLER__#########
        elif crawler_name in all_crawler_names:
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
                print(f"can not update crawler name: {crawler_name}")
        else:
            status_crawler_name = f"can not create and update: {crawler_name}"
            print(status_crawler_name)
            
    return {
        'statusCode': 200
        }