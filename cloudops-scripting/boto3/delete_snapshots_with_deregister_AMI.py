#This script reads a csv file of AWS Snapshots and deletes the snapshots.

import boto3
import botocore
import csv
from datetime import datetime
from dateutil.tz import tzlocal

accountCSV = 'test.csv'   #CSV with list of managed accounts.
snapshotCSV = 'adeleted_snapshots.csv' #CSV list with list of snapshotIDs and associated account number

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

#This function reads the csv file and generates a list of dictionaries which contain the account ID, a list of regions, and a list of snapshot IDs.
def acct_list(pathName):
    acctSnapshotList = []       #A list to store the snapshot IDs for each account.
    acctRegionList = []         #A list to store the region of each snapshot ID for each account.
    acctSnapshotOwnerList = []  #A list to store the owner of each snapshot ID for eacha account.
    acctSnapshotCostList = []   #A list to store the cost of each snapshot ID for each account.
    acctSnapshotAMIList = []    ##ADDED A list to store the AMI, if has any, of each snapshot ID for each account.
    accountDict = {'AccountID': '', 'Region': acctRegionList, 'SnapshotID': acctSnapshotList, 'Owner': acctSnapshotOwnerList, 'Cost': acctSnapshotCostList, 'AMI': acctSnapshotAMIList}       #defines the format of the dictionary
    currentAcct = ''
    accountList = []            #A list to store all the account dictionaries
    flag = False                
    with open(pathName, mode = 'r') as file:
        csvDict = csv.DictReader(file)          #reads the csv file and puts each line of data into a dictionary.
        infoCSV = list(csvDict)                 #creates a list of dicitonaries
        for line in infoCSV:                    #iterates through the list of dictionaries
            currentAcct = line['Owner Id']
            for acct in accountList:            #iterates through the currently stored account dictionaries.
                if acct['AccountID'] == currentAcct:    #checks if an account dictionary already exists adds the region and snapshot data if it does.
                    acct['Region'].append(line['Region Name'])      #Adds the current region value to the end of the existing list.
                    acct['SnapshotID'].append(line['Snapshot Id'])
                    acct['Owner'].append(line['owner2'])
                    acct['Cost'].append(line[' Cost - May 2024 '])
                    acct['AMI'].append(line['AMI']) #ADDED
                    flag = True
                    break                               #exits the loop once the account data has been recorded.
                else:
                    flag = False                #if the account is not found in the current list, then set flag to create a new account dictionary.  
            if not flag:                        #Creates an account dictionary for new accounts and adds it to the list. 
                acctSnapshotList = []
                acctRegionList = []
                acctSnapshotOwnerList = []
                acctSnapshotCostList = []
                acctSnapshotAMIList = []  #ADDED
                accountDict = {'AccountID': '', 'Region': acctRegionList, 'SnapshotID': acctSnapshotList, 'Owner': acctSnapshotOwnerList, 'Cost': acctSnapshotCostList, 'AMI': acctSnapshotAMIList}   #Sets the format of the account dictionary.
                accountDict.update(AccountID= currentAcct)
                acctRegionList.append(line['Region Name'])
                acctSnapshotList.append(line['Snapshot Id'])
                acctSnapshotOwnerList.append(line['owner2'])
                acctSnapshotCostList.append(line[' Cost - May 2024 ']) 
                acctSnapshotAMIList.append(line['AMI']) #ADDED
                accountDict.update(Region= acctRegionList, SnapshotID = acctSnapshotList, Owner = acctSnapshotOwnerList, Cost = acctSnapshotCostList, AMI = acctSnapshotAMIList)       #updates the account dictionary with the list of regions and list of snapshots
                accountList.append(accountDict)     #adds the account dictionary to the existing list of account dictionaries.
                flag = True  

    return accountList      #returns the list of account dictionaries.

#This function runs a simple command to validate authentication into the AWS Account.
def validate_acct(session):
    try:
        ec2 = session.client('ec2')
        regions = ec2.describe_regions()        #random api call to check if the created role can authenticate into the AWS Account.
        print('Validation success!')
        return True
    except Exception as validationError:
        print(f"A validation error has occurred: {validationError}")
        return False

#This function creates a list of dictionaries of snapshots for each region of the current account.
def regional_snapshots(acctSnapshots): 
        try:
            regionSnapshotList = []
            regionOwnerList = []
            regionCostList = []
            regionAMIList = [] #ADDED
            regionDict = {'Region': '', 'Snapshots': regionSnapshotList, 'Owners': regionOwnerList, 'Costs': regionCostList, 'AMI': regionAMIList}     #Sets the format of the region dictionary.
            currentRegion = ''
            regionList = [] #list of dictionaries with region and list of associated snapshots.

            snapshots = acctSnapshots['SnapshotID']     #Stores the list of snapshots associated with the current account
            owners = acctSnapshots['Owner']
            costs = acctSnapshots['Cost']
            regions = acctSnapshots['Region']           #Stores the list of regions that each snapshot is located in.
            AMI = acctSnapshots['AMI']                  #ADDED: Store the AMIs, if any, that the snapshot was created for
            counter = len(snapshots)                    #Gets the number of snapshots stored in the list.
            
            flag = False
            for iterator in range(counter):             #Creates a loop to iterate through the entire list of snapshots and regions.
                currentRegion = regions[iterator]
                for region in regionList:               #iterates through the currently stored region dictionaries.
                    if region['Region'] == currentRegion:       #Checks if the region dictionary already exists and adds the snapshot data if it does.
                        region['Snapshots'].append(snapshots[iterator])     #The 'iterator' value ensures that the region-snapshot pairing is retained.
                        region['Owners'].append(owners[iterator])
                        region['Costs'].append(costs[iterator])
                        region['AMI'].append(AMI[iterator])
                        flag = True
                        break                           #exits the loop once the region data has been recorded.
                    else:
                        flag = False                    #If region not found in current list then set flag to create new region dictionary.
                if not flag:                            #Creates a region dictionary for new regions and adds it to the list
                    regionSnapshotList = []
                    regionOwnerList = []
                    regionCostList = []
                    regionAMIList = [] #ADDED
                    regionDict = {'Region': '', 'Snapshots': regionSnapshotList, 'Owners': regionOwnerList, 'Costs': regionCostList, 'AMI': regionAMIList}        #Sets the format of the dictionary.
                    regionDict.update(Region = currentRegion)
                    regionSnapshotList.append(snapshots[iterator])
                    regionOwnerList.append(owners[iterator])
                    regionCostList.append(costs[iterator])
                    regionAMIList.append(AMI[iterator]) #ADDED
                    regionDict.update(Snapshots = regionSnapshotList, Owners = regionOwnerList, Costs = regionCostList, AMI = regionAMIList)                   #Updates the region dictionary with the list of snapshots
                    regionList.append(regionDict)                                       #Adds the region dictionary to the existing list of region dictionaries.
                    flag = True

            return regionList       #returns the list of region dictionaries.

        except Exception as errorSS:
            print(f"An error has occurred in region: {errorSS}") 

#Check if ami was used to launch an active EC2 instance
#ADDED
def ami_inUse(client, imageId):
    instances = client.describe_instances()
    for reservation in instances['Reservations']: #This loop looks for EC2 instances that were launched with the ami and skips deletion if an instance is found.
        instance = reservation['Instances'][0]  #information on image for EC2 instance.
        if instance['ImageId'] == imageId:
            print(f"{instance['ImageId']} was used to launch an existing EC2 instance")
            return True
    return False

#Deregisters an AMI based on the image_id and returns the success status
#ADDED
def deregister_ami(ec2,imageID):
    try:
        ec2.deregister_image(ImageId = imageID)
        print(f"AMI ID: {imageID} has been successfully deregistered!")
        return True
    except Exception as errorDA:
        print(f"An error with ami deregistration has occurred: {errorDA}")
        return False
    
#Deregister all AMIs associated with snapshot, if all deregistered, return TRUE
#ADDED
def deregister_all_amis(ec2, ami_list,):
    for ami in ami_list:
        response = ec2.describe_images(ImageIds=[ami])
        ami_image = response['Images'][0]
        state = ami_image.get('State', 'Unknown')
        if state == 'available':
            if not ami_inUse(ec2, ami): # ADDED If AMI not in use, deregister AMI
                success = deregister_ami(ec2, ami)
                if not success: #return false if failed to deregister any AMI
                    return False
            else: # ADDED If AMI is in use, cannot deregister AMI so cannot delete snapshot, return False
                return False
    return True

#This function makes the API call to delete snapshots using the snapshotID.
def delete_snapshots(session, snaps,acct,writeCsv):
    regionDictionaries = snaps
    for region in regionDictionaries:
        ec2 = session.client('ec2', region_name =  region['Region'])    #Creates the EC2 client that will make the API calls
        print(f"\n ", region['Region'])
        iterator = 0
        for snap in region['Snapshots']:    #Iterates through the list of snapshotIDs in the region.
            try:
                AMI_list = []
                owner = region['Owners'][iterator]              #Ensures the owner selected from the list matches the correct snapshot. 
                cost = region['Costs'][iterator]
                AMI_string = region['AMI'][iterator] #ADDED 
                print(len(region['Snapshots']))
                print(len(region['AMI']))
                print(len(AMI_list))
                AMI_list = AMI_string.split('; ') #ADDED separate AMIs as list of all AMIs is separated by ; (destination, source)
                print(len(AMI_list))
                print(AMI_list)
                if AMI_list !=  ['']: # ADDED if the snapshot is associated with at least 1 AMI, deregister the AMIs before deleting snapshot
                    success_deregister = deregister_all_amis(ec2, AMI_list)
                    if not success_deregister: #If unable to deregister any AMIs, cannot proceed with deleting the snapshot
                        print(f'{snap} cannot be deleted as associated with active EC2 AMI')
                        acctValidData = [acct, region['Region'], snap, 'Success', 'Failed', owner, cost]     #The format of the output data if the deletion fails.
                        writeCsv.writerow(acctValidData)
                        iterator += 1
                        break      
                ec2.delete_snapshot(SnapshotId = snap)         #The API call that deletes a snapshot based on the snapshot ID.
                print(f'{snap} has been successfully deleted!!!')
                acctValidData = [acct, region['Region'], snap, 'Success', 'Success', owner, cost]    #The format of the output data if the deletion is successful.
                writeCsv.writerow(acctValidData)                                        #Writes the output to the specfied csv file.
                iterator += 1

            except Exception as deleteError:
                print(f"An deletion error has occurred: {deleteError}")
                acctValidData = [acct, region['Region'], snap, 'Success', 'Failed', owner, cost]     #The format of the output data if the deletion fails.
                writeCsv.writerow(acctValidData)
                iterator += 1

#The main block of code
def main():
    try:
        with open(snapshotCSV, mode = 'w', newline='')	as file:        #Open new file to write output data.
            header = ['AcctID', 'Region', 'SnapshotID', 'AcctAccessStatus', 'DeletionStatus', 'Owner', 'Cost']    #Column headings in csv files.
            deletedSnaps = csv.writer(file, delimiter=',')
            deletedSnaps.writerow(header)       #Writes the header to the top of the csv file.
            accounts = acct_list(accountCSV)    #Stores the list of account dictionaries.
            for account in accounts:            #Iterates through the list of account dictionaries.  
                accountNumber = account['AccountID']
                accountARN = f'arn:aws:iam::{accountNumber}:role/AWSCloudFormationStackSetExecutionRole'    #creates the role required to authenticate into AWS
                print('\n')
                print(accountARN)
                session = assumed_role_session(accountARN)
                validationStatus = validate_acct(session)       #returns the validation status of authenticating into the AWS Account.
                if validationStatus:            #If authentication is successful then proceed with deletion process.
                    snapshots = regional_snapshots(account)
                    delete_snapshots(session, snapshots, accountNumber, deletedSnaps)
                else:                           #If authentication fails then log authentication status as "Failed" in output data csv.
                    counter = len(account['Region'])
                    for iterator in range(counter): 
                        acctData = [accountNumber, account['Region'][iterator], account['SnapshotID'][iterator], 'Failed','N/A', account['Owner'][iterator], account['Cost'][iterator]]
                        deletedSnaps.writerow(acctData)
            
    except Exception as e:
        print(f"An overall error has occurred: {e}")

if __name__ == "__main__":
    main()