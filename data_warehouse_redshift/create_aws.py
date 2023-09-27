"""
AWS Resource Management Script

Author: SharmaRupali (https://github.com/SharmaRupali)

This script provides functions to create and delete AWS resources. 
Ensure that all the required properties are properly configured in 'dwh.cfg' before executing this script.
"""

import boto3
import configparser
import json

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

REGION = str(config.get('CLUSTER_CONFIG', 'REGION'))
KEY = config.get('AWS','KEY')
SECRET = config.get('AWS','SECRET')
IAM_ROLE_NAME = config.get('AWS', 'IAM_ROLE_NAME')


iam = boto3.client('iam', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
ec2 = boto3.resource('ec2', region_name=REGION, aws_access_key_id=KEY, aws_secret_access_key=SECRET)


def create_iam_role(role_name):
    """
    Create an AWS IAM Role.

    This function creates an AWS Identity and Access Management (IAM) Role with the specified name and policies, 
    allowing Redshift clusters to call AWS services on our behalf.

    Parameters:
    role_name (str): The name of the IAM Role to create.

    Returns:
    str: The ARN (Amazon Resource Name) of the created IAM Role.

    Example:
    iam_role_arn = create_iam_role(IAM_ROLE_NAME)
    """
    
    print("Creating IAM Role...")

    iam_role = iam.create_role(
        Path='/',
        RoleName=role_name,
        Description='Allows Redshift clusters to call AWS services on your behalf.',
        AssumeRolePolicyDocument=json.dumps({
            'Statement': [{
                'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}
            }],
            'Version': '2012-10-17'
        })
    )

    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']

    iam_role_arn = iam.get_role(RoleName=role_name)['Role']['Arn']

    print("...IAM Role Created!")

    return iam_role_arn


def create_cluster(role_arn, config):
    """
    Create an Amazon Redshift Cluster.

    This function creates an Amazon Redshift cluster with the specified configuration and associates 
    it with the provided IAM Role.

    Parameters:
    role_arn (str): The ARN (Amazon Resource Name) of the IAM Role to associate with the cluster.
    config (configparser.ConfigParser): Configuration settings read from 'dwh.cfg'.

    Returns:
    None

    Example:
    create_cluster(iam_role_arn, config)
    """
    
    print("Creating Redshift Cluster...")

    response = redshift.create_cluster(
        ClusterType=config.get('CLUSTER_CONFIG', 'CLUSTER_TYPE'),
        NodeType=config.get('CLUSTER_CONFIG', 'NODE_TYPE'),
        NumberOfNodes=int(config.get('CLUSTER_CONFIG', 'NUM_NODES')),
        ClusterIdentifier=config.get('CLUSTER_CONFIG', 'CLUSTER_IDENTIFIER'),
        
        DBName=config.get('CLUSTER', 'DB_NAME'),
        MasterUsername=config.get('CLUSTER', 'DB_USER'),
        MasterUserPassword=config.get('CLUSTER', 'DB_PASSWORD'),
        
        IamRoles=[role_arn]  
    )

    print("...Redshift Cluster Created!")


def create_vpc(myClusterProps, port):
    """
    Open a TCP Port for cluster endpoints in the VPC.

    This function opens a specified TCP port for cluster endpoints within the Virtual Private Cloud (VPC).

    Parameters:
    myClusterProps (dict): The cluster properties.
    port (str): The TCP port number to open.

    Returns:
    None

    Example:
    create_vpc(myClusterProps, '5439')
    """
    
    print("Opening TCP Port for cluster endpoints...")
    
    vpc = ec2.Vpc(id=myClusterProps['VpcId'])
    defaultSg = list(vpc.security_groups.all())[0]
    print(defaultSg)
    defaultSg.authorize_ingress(
        GroupName=defaultSg.group_name,
        CidrIp='0.0.0.0/0',
        IpProtocol='TCP',
        FromPort=int(port),
        ToPort=int(port)
    )
    print(f"...TCP Port {port} Opened!")


def describe(cluster_identifier):
    """
    Describe an Amazon Redshift Cluster.

    This function retrieves and displays information about the specified Amazon Redshift cluster, 
    such as its endpoint and associated IAM Role ARN.

    Parameters:
    cluster_identifier (str): The identifier of the Redshift cluster.

    Returns:
    None

    Example:
    describe('my-redshift-cluster')
    """
    
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        
    print(f"Cluster Endpoint: {myClusterProps['Endpoint']['Address']}")
    print(f"IAM Role ARN: {myClusterProps['IamRoles'][0]['IamRoleArn']}")


def main(action):
    """
    Main function to manage AWS resources.

    This function is the main entry point for managing AWS resources. It performs actions based on the specified 'action' parameter, 
    including creating, describing, creating VPC rules for, or deleting AWS resources.

    Parameters:
    action (str): The action to perform ('create', 'describe', 'create_vpc', 'delete').

    Returns:
    None

    Example:
    main('create')
    """
    
    if action == "create":
        iam_role_arn = create_iam_role(IAM_ROLE_NAME)
        create_cluster(iam_role_arn, config)
    elif action == "describe":
        describe(config.get('CLUSTER_CONFIG', 'CLUSTER_IDENTIFIER'))
    elif action == "create_vpc":
        myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get('CLUSTER_CONFIG', 'CLUSTER_IDENTIFIER'))['Clusters'][0]
        create_vpc(myClusterProps, config.get('CLUSTER', 'DB_PORT'))
    elif action == "delete":
        print("Deleting Redshift Cluster...")
        redshift.delete_cluster( ClusterIdentifier=config.get('CLUSTER_CONFIG', 'CLUSTER_IDENTIFIER'),  SkipFinalClusterSnapshot=True)
        print("Redshift Cluster Deleted...")
        
        print("Deleting IAM Role...")
        iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        iam.delete_role(RoleName=IAM_ROLE_NAME)
        print("...IAM Role Deleted!")



if __name__ == "__main__":
    main("create")