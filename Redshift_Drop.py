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
def Drop_Cluster(redshift, DWH_CLUSTER_IDENTIFIER):
    """
    This function will be responsible for deleting the cluster.
    """    
    redshift.delete_cluster( ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)

def main():
    
    # Get basic configure
    KEY, SECRET, DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT, DWH_IAM_ROLE_NAME, redshift_Configure = get_config('/home/workspace/dwh.cfg')
    print(redshift_Configure)
    
    # Create ec2, s3, iam, redshift resource and client 
    iam = Create_Object_Client('iam', 'us-west-2', KEY, SECRET)
    redshift = Create_Object_Client('redshift', 'us-west-2', KEY, SECRET)
    Drop_Cluster(redshift, DWH_CLUSTER_IDENTIFIER)
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    cluster_status = prettyRedshiftProps(myClusterProps)
    while cluster_status.iloc[2,1] =='deleting':
        print('We are still working on deleting the cluster!')
        time.sleep(60)
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        cluster_status = prettyRedshiftProps(myClusterProps)
        if not redshift.describe_clusters()['Clusters']:
            break
    print ('Cluster is dropped, please go ahead!')
    
    iam.detach_role_policy(RoleName=DWH_IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=DWH_IAM_ROLE_NAME)
    print('Role is dropped, please go ahead!')
if __name__ == "__main__":
    main()
