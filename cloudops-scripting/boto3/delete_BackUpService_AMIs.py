#Aim: locate and delete old AMIs created by AWS Backup Service in entire cloud environment
#Steps: 1. Gather list of old AMIs with snapshots - completed
#       2. Delete automatically generated AMI backups (recovery points) - completed

import boto3
import botocore
import csv
from datetime import datetime
from dateutil.tz import tzlocal

accountCSV = 'auto_Corporate_accounts.csv'   #CSV with list of managed accounts.
backupCSV = '_test_amibackups_May3.csv'   #CSV to be created for each account.
daysChecked = 180                       #checks for resources greater than the number of days
keyword = "AwsBackup"                 #variable to search for the keyword in the ami-image name.
volumeCost = 0.05                       #Cost of storing volume in US regions.
costSavings = 0.00

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

#Check if ami was used to launch an active EC2 instance
def ami_inUse(client, imageId):
    instances = client.describe_instances()
    for reservation in instances['Reservations']: #This loop looks for EC2 instances that were launched with the ami and skips deletion if an instance is found.
        instance = reservation['Instances'][0]  #information on image for EC2 instance.
        if instance['ImageId'] == imageId:
            print(f"{instance['ImageId']} was used to launch an existing EC2 instance")
            return True
    return False

#Delete recovery point of AMI in AWS Backup Vault
def delete_repoint(session, imageName, regionName):
    try:
        backup = session.client('backup', region_name = regionName)
        listVaults = backup.list_backup_vaults()        #obtains a list of the backup vaults in the account and specific region.
        for vault in listVaults['BackupVaultList']:     #This loop gets the specific vault name and looks for EC2 recovery points in that vault.
            vaultName = vault['BackupVaultName']
            listRPs = backup.list_recovery_points_by_backup_vault(BackupVaultName = vaultName, ByResourceType = 'EC2')
            for recoveryPoint in listRPs['RecoveryPoints']:     #This loop looks for the recovery point that matches the ami backup identified for deletion.
                if imageName in recoveryPoint['RecoveryPointArn']:
                    rpName = recoveryPoint['RecoveryPointArn']
                    #backup.delete_recovery_point(BackupVaultName = vaultName, RecoveryPointArn = rpName)
                    print(f'The Backup Service has deleted recovery point {rpName} associated with {imageName} in {vaultName}.')
                    return True
    except Exception as errorDRP:
        print(f"An error with recovery point deletion has occurred: {errorDRP}")
        return False

#Locates old AMI backups in managed accounts and stores the AMI IDs and associated snapshot IDs in a CSV.
def ami_backups(session,pathName,oldDays,substring):
    with open(pathName, mode = 'w', newline='')	as file:
        header = ['Name', 'ImageID', 'ImageLocation', 'CreationDate', 'Region', 'OwnerID', 'SnapshotID', 'Volume Size', 'Cost Savings', 'RPDeletionStatus']    #Column headings in csv files.
        amiBackups = csv.writer(file, delimiter=',')
        amiBackups.writerow(header)     #writes the column headings to the top of the file
        regions = get_all_regions(session)
        for region in regions:      #Iterates through each region in the managed accounts with the exception of "ap-southeast-4".
            print(f"\n ", region)
            if region == "ap-southeast-4":
                continue
            ec2 = session.client('ec2', region_name = region)
            images = ec2.describe_images(Owners=['self'])   #The 'self' value scopes the images to just the specified account.
            for ami in images['Images']:        #Iterates through all the AMIs in an account and stores AMI IDs older than the days specified.
                amiName = ami['Name']
                amiId = ami['ImageId']
                if amiId == 'ami-0af8f6b15e751fa6d':    #Skip particular amis if necessary
                    print(f'Skipping {amiId}!!!')
                    continue
                if substring.lower() in amiName.lower():    #Checks if ami name meets the search criteria.
                    dateString = datetime.strptime(ami['CreationDate'], '%Y-%m-%dT%H:%M:%S.000Z')   #Converts the date to a usable format for calculations.
                    timeDiff = datetime.now() - dateString
                    if timeDiff.days > oldDays:
                        amiUsed = ami_inUse(ec2,amiId)
                        if not amiUsed:     #If the ami is not used to launch an active instance then proceed with deletion.
                            print(f'Beginning process to delete recovery point associated with {amiId}')
                            deleteRP = delete_repoint(session, amiId, region) #this uses same ami id multiple times as an ami might have many attached devices
                            for ebs in ami['BlockDeviceMappings']:  #Iterates through AMI's devices
                                if (ebs.get('Ebs')):    #Checks if an AMI has associated snapshots.
                                    costSavings = ebs['Ebs']['VolumeSize'] * volumeCost
                                    amiData = [ami['Name'], ami['ImageId'], ami['ImageLocation'], ami['CreationDate'], region, ami['OwnerId'], ebs['Ebs']['SnapshotId'], ebs['Ebs']['VolumeSize'], costSavings, deleteRP, ]
                                    amiBackups.writerow(amiData)
                        else:
                            print(f'This AMI ID: {amiId} was used to launch an active EC2 instance')


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

    
def main():
    try:
        accountNumber = read_csv(accountCSV)
        for account in accountNumber:
            accountARN = f'arn:aws:iam::{account}:role/AWSCloudFormationStackSetExecutionRole'
            print(accountARN)
            session = assumed_role_session(accountARN)
            prefix = str(account) #Helps to create a separate csv file of ami backups for each account.
            ami_backups(session,(prefix + backupCSV), daysChecked, keyword)
            
    except Exception as e:
        print(f"An error has occurred: {e}")

if __name__ == "__main__":
    main()