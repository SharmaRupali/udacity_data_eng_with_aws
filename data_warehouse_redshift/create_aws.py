"""
@Author: SharmaRupali (https://github.com/SharmaRupali)

This script has functions to create new AWS resources and delete the exisitng ones.
Please make sure all the properties are filled in 'dwh.cfg' before executing this script
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


def create_iam_role():
    """
    Creates an IAM role and attaches policies to it.
    """
    print("Creating IAM Role...")

    iam_role = iam.create_role(
        Path='/',
        RoleName=IAM_ROLE_NAME,
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
        RoleName=IAM_ROLE_NAME,
        PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess'
    )['ResponseMetadata']['HTTPStatusCode']

    iam_role_arn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    print("...IAM Role Created!")

    return iam_role_arn


def create_cluster(role_arn):
    """
    Creates a Redshift cluster.
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
    Opens a TCP port for cluster endpoints.
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
    Describes the cluster details.
    """
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        
    print(f"Cluster Endpoint: {myClusterProps['Endpoint']['Address']}")
    print(f"IAM Role ARN: {myClusterProps['IamRoles'][0]['IamRoleArn']}")


def main(action):
    if action == "create":
        iam_role_arn = create_iam_role()
        create_cluster(iam_role_arn)
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