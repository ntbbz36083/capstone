def get_config(configu_file):
    config = configparser.ConfigParser()
    config.read_file(open(configu_file))

    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE = config.get('DWH','DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH','DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH','DWH_NODE_TYPE')

    DWH_CLUSTER_IDENTIFIER = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    redshift_Configure = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
    return KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, redshift_Configure

def Create_Object_Resource(object_type, region_name_full, KEY, SECRET):
    
    ob = boto3.resource(object_type,
                       region_name=region_name_full,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    return ob

def Create_Object_Client(object_type, region_name_full, KEY, SECRET):
    
    ob = boto3.client(object_type,
                       region_name=region_name_full,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    return ob

def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

def Create_Cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    redshift.create_cluster(        
        ClusterType=DWH_CLUSTER_TYPE,
        NodeType=DWH_NODE_TYPE,
        NumberOfNodes=int(DWH_NUM_NODES),

        #Identifiers & Credentials
        DBName=DWH_DB,
        ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
        MasterUsername=DWH_DB_USER,
        MasterUserPassword=DWH_DB_PASSWORD,

        #Roles (for s3 access)
        IamRoles=[roleArn]  
    )

def main():
    import time
    import pandas as pd
    import boto3
    import json
    import configparser
    import psycopg2
    from botocore.exceptions import ClientError
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, redshift_Configure = get_config('/home/workspace/dwh.cfg')
    print(redshift_Configure)
    
    ec2 = Create_Object_Resource('ec2', 'us-west-2', KEY, SECRET)
    s3 = Create_Object_Resource('s3', 'us-west-2', KEY, SECRET)
    iam = Create_Object_Client('iam', 'us-west-2', KEY, SECRET)
    redshift = Create_Object_Client('redshift', 'us-west-2', KEY, SECRET)
    
    #ec2 = Create_Object_Resource('ec2', 'us-east-1', KEY, SECRET)
    #s3 = Create_Object_Resource('s3', 'us-east-1', KEY, SECRET)
    #iam = Create_Object_Client('iam', 'us-east-1', KEY, SECRET)
    #redshift = Create_Object_Client('redshift', 'us-east-1', KEY, SECRET)

    #1.1 Create the role, 
    try:
        print("1.1 Creating a new IAM Role") 
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
            {'Statement': [{'Action': 'sts:AssumeRole',
               'Effect': 'Allow',
               'Principal': {'Service': 'redshift.amazonaws.com'}}],
             'Version': '2012-10-17'})
            )    
    except Exception as e:
        print(e)
    
    
    print("1.2 Attaching Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']

    print(roleArn)
    
    print("1.4 Creating Cluster")
    Create_Cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = prettyRedshiftProps(myClusterProps)
    if cluster_status.iloc[2,1] =='available':
        print('Cluster is available now!')
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", roleArn)
    else:    
        flag = True
        i= 0
        while flag:
            myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
            cluster_status = prettyRedshiftProps(myClusterProps)
            if cluster_status.iloc[2,1] =='creating':
                print('We are still working on creating the cluster, approximate {}/20 done!'.format(i))
                i += 1
                time.sleep(30)
            else:
                flag = False
                print('cluster is available!')    
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
        print("DWH_ROLE_ARN :: ", roleArn)
    
    
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(DWH_PORT),
        ToPort=int(DWH_PORT)
                                )
    except Exception as e:
        if e:
            print('Policy created, you can go ahead!')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
if __name__ == "__main__":
    main()
