# Define a function that get basic config
def get_config(configu_file):
    """
    This function will be responsible for getting the configration.
    """
    # Load in the cofigure file
    config = configparser.ConfigParser()
    config.read_file(open(configu_file))
    # Get the key and secret key
    KEY = config.get('AWS','KEY')
    SECRET = config.get('AWS','SECRET')
    # Get the cluster tyep
    DWH_CLUSTER_TYPE = config.get('DWH','DWH_CLUSTER_TYPE')
    DWH_NUM_NODES = config.get('DWH','DWH_NUM_NODES')
    DWH_NODE_TYPE = config.get('DWH','DWH_NODE_TYPE')
    # Get database parameter
    DWH_CLUSTER_IDENTIFIER = config.get('DWH','DWH_CLUSTER_IDENTIFIER')
    DWH_DB = config.get("DWH","DWH_DB")
    DWH_DB_USER = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
    #Generate a pandas dataframe that will display all the configure
    redshift_Configure = pd.DataFrame({"Param":
                  ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES", "DWH_NODE_TYPE", "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER", "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
              "Value":
                  [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME]
             })
    # Return everyting
    return KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, redshift_Configure

# Define a function that create object resource 
def Create_Object_Resource(object_type, region_name_full, KEY, SECRET):
    """
    This function will be responsible for creating the Object Resource.
    """
    ob = boto3.resource(object_type,
                       region_name=region_name_full,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    return ob

# Define a function that create object client 
def Create_Object_Client(object_type, region_name_full, KEY, SECRET):
    """
    This function will be responsible for creating the Object Client.
    """
    ob = boto3.client(object_type,
                       region_name=region_name_full,
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET
                    )
    return ob

# Define a function that get redshift cluster information and return it as a pandas dataframe
def prettyRedshiftProps(props):
    """
    This function will be responsible for creating a dataframe that contains cluster's parameters.
    """
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])

# Define a function thatcreate redshift cluster with configure
def Create_Cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn):
    """
    This function will be responsible for creating the cluster.
    """
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
    # Import liraries
    import time
    import pandas as pd
    import boto3
    import json
    import configparser
    import psycopg2
    from botocore.exceptions import ClientError
    
    # Get basic configure
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, redshift_Configure = get_config('/home/workspace/dwh.cfg')
    print(redshift_Configure)
    
    # Create ec2, s3, iam, redshift resource and client 
    ec2 = Create_Object_Resource('ec2', 'us-west-2', KEY, SECRET)
    s3 = Create_Object_Resource('s3', 'us-west-2', KEY, SECRET)
    iam = Create_Object_Client('iam', 'us-west-2', KEY, SECRET)
    redshift = Create_Object_Client('redshift', 'us-west-2', KEY, SECRET)

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

    # Create cluster and return DWH_ENDPOINT and DWH_ROLE_ARN
    x = redshift.describe_clusters()['Clusters']
    if not x: 
        Create_Cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE, DWH_NUM_NODES, DWH_DB, DWH_CLUSTER_IDENTIFIER, DWH_DB_USER, DWH_DB_PASSWORD, roleArn)
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
                print('cluster is already available!')    
                DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
                DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
                print("DWH_ENDPOINT :: ", DWH_ENDPOINT)
                print("DWH_ROLE_ARN :: ", roleArn)
    else:
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = prettyRedshiftProps(myClusterProps)
        if cluster_status.iloc[2,1] =='available':
            print('Cluster is available now!')
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
