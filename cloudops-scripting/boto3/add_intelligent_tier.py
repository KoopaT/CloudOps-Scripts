#Adds lifecycle rule to entire bucket but overwrites/deletes all exisitng lifecycle rules.
#Options: Get exisitng lifecycle configurations for buckets and append to new rule. OR skip buckets with lifecycle rules

import boto3
import botocore
import csv
from datetime import datetime
from dateutil.tz import tzlocal

accountCSV = 's3_accounts.csv'   #CSV with list of managed accounts.
s3Bucket = '_bucketslist.csv'
lcrCSV = '_intelligenttier.csv'
lcName = 'MoveToIntelligentTiering'

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

#Checks to see if the bucket has exisiting lifecycle rules to avoid overwriting rules.
def check_lifecycle(session, bucket): 
    bucketClient = session.client('s3')
    try:
        response = bucketClient.get_bucket_lifecycle_configuration(
            Bucket = bucket,
        )
        return True
    except Exception as noLifecycle:
        print(f"{noLifecycle} in the bucket {bucket}")
        return False

#Adds the lifecycle rule for intelligent tiering to the bucket.
def bucket_lifecycle(session, prefixCsv,accountNum):
    s3Client = session.client('s3')
    pathName = prefixCsv + lcrCSV
    bucketCSV = prefixCsv + s3Bucket
    with open(pathName, mode = 'w', newline='')	as file:
        header = ['Account#', 'BucketName', 'LifecycleName', 'RuleApplied','OwnerID']    #Column headings in csv files.
        lcrIntelliTier = csv.writer(file, delimiter=',')
        lcrIntelliTier.writerow(header)
        buckets = read_csv(bucketCSV)       #reads the list of buckets associated with an AWS account from a user-defined csv file.
        for bucketName in buckets:          #interates throught the buckets to apply the lifecycle rule.
            existingLC = check_lifecycle(session, bucketName)
            if not existingLC:              #If a bucket has no exisitng lifecycle rules add the new one, otherwise skip to the next bucket.
                response = s3Client.put_bucket_lifecycle_configuration(
                    Bucket = bucketName,
                    LifecycleConfiguration = {
                        'Rules': [
                            {
                                'ID': lcName,       #Name of the Lifecycle rule
                                'Prefix': '',                           #Empty prefix name to ensure the lifecycle rule applies to all objects in the bucket.
                                'Status': 'Disabled',                   #Enable or disable this rule
                                'Transitions': [
                                    {
                                        'Days': 0,                      #The number of days before the object is transitioned. 0 days = instantly/end of current day
                                        'StorageClass': 'INTELLIGENT_TIERING'
                                    },
                                ]
                            },
                        ]
                    },
                )
                lcrData = [accountNum, bucketName, lcName, (not existingLC), 'owner']
                lcrIntelliTier.writerow(lcrData)        #writes the data to a csv file.
                print(f"Lifecycle rule was successfully added to {bucketName}!!!")
            else:
                lcrData = [accountNum, bucketName, lcName, (not existingLC), 'owner']
                lcrIntelliTier.writerow(lcrData)        #writes the data to a csv file.
                print(f"The bucket {bucketName} has existing an lifecycle rule and will be skipped!")


def main():
    try:
        accountNumber = read_csv(accountCSV)
        for account in accountNumber:
            accountARN = f'arn:aws:iam::{account}:role/AWSCloudFormationStackSetExecutionRole'
            print(accountARN)
            session = assumed_role_session(accountARN)
            prefix = str(account) #Helps to create a separate csv file of ami backups for each account.
            bucket_lifecycle(session, prefix,account)
            
            
            
    except Exception as e:
        print(f"An error has occurred: {e}")

if __name__ == "__main__":
    main()