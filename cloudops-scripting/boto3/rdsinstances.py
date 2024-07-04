import boto3
import botocore
import csv
from datetime import datetime
from dateutil.tz import tzlocal


accountCSV = 'prod_Corporate_accounts.csv'   #CSV with list of managed accounts.
multiAZ = '_prod_multiAZ_rds.csv'

def assumed_role_session(role_arn: str, base_session: botocore.session.Session = None):
    base_session = base_session or boto3.session.Session()._session
    fetcher = botocore.credentials.AssumeRoleCredentialFetcher(
        client_creator = base_session.create_client,
        source_credentials = base_session.get_credentials(),
        role_arn = role_arn,
        extra_args = {
        #    'RoleSessionName': None # set this if you want something non-default
        }
    )
    creds = botocore.credentials.DeferredRefreshableCredentials(
        method = 'assume-role',
        refresh_using = fetcher.fetch_credentials,
        time_fetcher = lambda: datetime.now(tzlocal())
    )
    botocore_session = botocore.session.Session()
    botocore_session._credentials = creds
    return boto3.Session(botocore_session = botocore_session)

#Return all regions available to the EC2 clients.
def get_all_regions(session):
    ec2 = session.client('ec2')
    return [region['RegionName'] for region in ec2.describe_regions()['Regions']]

#Reads a csv file of managed accounts and returns the account numbers in a list.
def read_csv(pathName):
    accountNum = []     #List to store account numbers.
    lineCount = 0
    with open(pathName, mode = 'r') as file:
        accountsList = list(csv.reader(file))
        for acct in accountsList:   #Reads the csv file with managed accounts and appends the account numbers to a list.
            if lineCount == 0:
                lineCount += 1
                continue
            accountNum.append(acct[0])
    return accountNum

def rds_multiAZ(session, multiAZcsv):
    with open(multiAZcsv, mode = 'w', newline='') as file:
        header = ['DBIdentifier', 'MultiAZStatus']
        multiAzRds = csv.writer(file, delimiter=',')
        multiAzRds.writerow(header)
        regions = get_all_regions(session)
        for region in regions:
            print(f"\n", region)
            if region == "ap-southeast-4":
                continue
            rds = session.client('rds', region_name = region)
            instances = rds.describe_db_instances()
            for db in instances['DBInstances']:
                multiAzStatus = db['MultiAZ']
                if multiAzStatus == True:
                    rdsData = [db['DBInstanceIdentifier'], multiAzStatus]
                    multiAzRds.writerow(rdsData)
                    print("multiaz instance found")
                else:
                    print(f'This instance: {db['DBInstanceIdentifier']} is not a multiAZ instance')

def main():
    try:
        accountNumber = read_csv(accountCSV)
        for account in accountNumber:
            accountARN = f'arn:aws:iam::{account}:role/AWSCloudFormationStackSetExecutionRole'
            print(accountARN)
            session = assumed_role_session(accountARN)
            prefix = str(account) #Helps to create a separate csv file of ami backups for each account.
            rds_multiAZ(session, (prefix + multiAZ))
            
    except Exception as e:
        print(f"An error has occurred: {e}")

if __name__ == "__main__":
    main()