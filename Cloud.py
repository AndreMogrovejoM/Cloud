import json
import os
import boto3
import subprocess
from botocore.exceptions import ClientError

def create_movie_table(dynamodb=None):
    table_wcu = 20
    table_rcu = 20
    index_wcu = 30
    index_rcu = 20
    table = dynamodb.create_table(TableName='Videos', KeySchema=[{'AttributeName': '_id', 'KeyType': 'HASH'}],
    
                      ProvisionedThroughput={'ReadCapacityUnits': table_rcu, 'WriteCapacityUnits': table_wcu},
                      
                      AttributeDefinitions=[{'AttributeName': '_id', 'AttributeType': 'N'},
                      
                                            {u'AttributeName': u'label', u'AttributeType': u'S'},
                                            
                                            {u'AttributeName': u'titulo', u'AttributeType': u'S'},
                                            
                                            {u'AttributeName': u'keywords', u'AttributeType': u'S'},
                                            
                                            {u'AttributeName': u'labelts', u'AttributeType': u'S'}],
                                            
                      GlobalSecondaryIndexes=[
                          {'IndexName': 'label-index', 'Projection': {'ProjectionType': 'ALL'},
                          
                           'ProvisionedThroughput': {'WriteCapacityUnits': index_wcu,
                                                     'ReadCapacityUnits': index_rcu},
                                                     
                           'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'label'}]},
                           
                          {'IndexName': 'keywords-index', 'Projection': {'ProjectionType': 'ALL'},
                          
                           'ProvisionedThroughput': {'WriteCapacityUnits': index_wcu,
                                                     'ReadCapacityUnits': index_rcu},
                           'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'keywords'}]},
                           
                          {'IndexName': 'labelts-index', 'Projection': {'ProjectionType': 'ALL'},
                          
                           'ProvisionedThroughput': {'WriteCapacityUnits': index_wcu,
                                                     'ReadCapacityUnits': index_rcu},
                                                     
                           'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'labelts'}]},
                           
                          {'IndexName': 'titulo-index', 'Projection': {'ProjectionType': 'ALL'},
                          
                           'ProvisionedThroughput': {'WriteCapacityUnits': index_wcu,
                                                     'ReadCapacityUnits': index_rcu},
                                                     
                           'KeySchema': [{'KeyType': 'HASH', 'AttributeName': 'titulo'}]}])
    return table


def LabelsConfidence(Names,Confidences,dictionary):	
	for i in range(len(dictionary['Labels'])):
		if not (dictionary['Labels'][i]['Name'] in Names):
			Names.append(dictionary['Labels'][i]['Name'])
			Confidences.append( dictionary['Labels'][i]['Confidence'])

def generateStrings(Names,Confidences):
	Keywords = ""
	Presiciones = ""

	for i in range(len(Names)):
		if i == (len(Names)-1):
			Keywords = Keywords + Names[i]
			Presiciones = Presiciones + str(Confidences[i])
		else:
			Keywords = Keywords + Names[i] + ", "
			Presiciones = Presiciones + str(Confidences[i]) + ", "
	return Keywords, Presiciones

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    
    table = dynamodb.Table('Videos')

    try:
        is_table_existing = table.table_status in ("CREATING", "UPDATING",
                                                 "DELETING", "ACTIVE")
    except ClientError:
        is_table_existing = False
        print ("Table %s doesn't exist." % table.name)
        movie_table = create_movie_table(dynamodb)
        print("Table status:", movie_table.table_status)
        
        
    
    #table.put_item(TableName='Videos', Item={'_id':1,'label':'Gato','titulo':'Gatitos',
	#'keywords':'gatitos bien peludos','labelsts':'10:20'})
        
    
    s3 = boto3.client('s3')
    client  =  boto3.client('rekognition')
    # replace below configuration
    bucket_name = 'rekonition'
    key = 'video.mp4'

    # downloading file to /tmp directory within lambda
    lambda_file_path = f'/tmp/{key}'
    lambda_output_file_path = '/tmp/trancoded_musiquita.wav'
    
    # downloading file
    s3.download_file(bucket_name, key, lambda_file_path)
    
    out_movie = lambda_file_path
    
    frame = '/tmp/framecito.png'
    
    PrincipalName = ""
    Names = []
    Confidences = []
    for x in range(0,8):
        time = '00:00:0' + str(x) + '.000'
        varaible2= ' -ss ' + time +' -vframes 1 '
        
        os.system(f'/opt/ffmpeglib/ffmpeg -i '+ str(out_movie) + varaible2 + str(frame)  )
        s3.upload_file(frame,bucket_name,'framecito.png')
        fileObj = s3.get_object( Bucket = bucket_name ,  Key='framecito.png' )
        file_content =  fileObj["Body"].read( )
        
            
            
        labels = client.detect_labels(Image={"Bytes": file_content},MaxLabels = 3 ,   MinConfidence = 95 )
        if x == 0:
            PrincipalName = labels['Labels'][0]['Name']
        LabelsConfidence(Names,Confidences,labels)
        
    Name, Confidence = generateStrings(Names, Confidences)
    table.put_item(TableName='Videos', Item={'_id':x,'label':PrincipalName,'titulo':key,'keywords':Name,'labelsts':Confidence})
	
    try:    
        consulta = table.get_item(TableName='Videos', Key={'_id':7})
        print(consulta)
    except ClientError:
        consulta = "No se pudo wuacho"
        
    return {
        'statusCode': 200,
        'body': json.dumps("Code")
    }