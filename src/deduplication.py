# -*- coding: utf-8 -*-
"""
Created on Wed Feb  5 13:43:26 2020

@author: Usman
"""
import re
import csv
import os
import dedupe
from future.builtins import next
from unidecode import  unidecode


def transformation(listOfDevelopers):
    rows = []
    counter = 0
    # jira has uniuqe userid while git has unique emailid
    # here name = jira's display name or git name
    # here email = jira userid or git emailid  
    for developer in listOfDevelopers:
        counter += 1 
        name = developer['name'] 
        print(name)             
        name_Processed = name.replace('.','')
        name_Processed = name_Processed.replace('-','')
        name_Processed = re.sub(' [A-Z](?= )', '', name_Processed)
        name_Processed = re.sub('[^a-zA-Z0-9 \n\.]', '', name_Processed)
        name_Processed = ''.join(i for i in name_Processed if not i.isdigit())     
        email = developer['email']
       
        if "@" in email:
            email_Processed = email[:email.find('@')]
            email_Processed = email_Processed.replace('.',' ')
            email_Processed = email_Processed.replace('-',' ')
            email_Processed = email_Processed.replace('_',' ')
            email_Processed = email_Processed.replace('+',' ')
            if ''.join(i for i in email_Processed if not i.isdigit()):
                email_Processed = ''.join(i for i in email_Processed if not i.isdigit())
        else:
            email_Processed = email.replace('.',' ')
            email_Processed = email_Processed.replace('-',' ')
            email_Processed = email_Processed.replace('_',' ')
            email_Processed = ''.join(i for i in email_Processed if not i.isdigit()) 
        print(email_Processed) 
        for words in [email_Processed]:
            if re.search(r'\b' + words + r'\b', name_Processed, re.IGNORECASE):
                rows.append({'id':counter ,'DisplayName':name ,'Email':email ,'DisplayName_Processed':name_Processed , 'Email_Processed':email_Processed, 'Info':name_Processed })
            else: 
                if len(name_Processed.split()) > 1:
                    rows.append({'id':counter ,'DisplayName':name ,'Email':email ,'DisplayName_Processed':name_Processed , 'Email_Processed':email_Processed, 'Info':(name_Processed[0]+name_Processed[name_Processed.rfind(' ')+1:]) })
                else:
                    rows.append({'id':counter ,'DisplayName':name ,'Email':email, 'DisplayName_Processed':name_Processed , 'Email_Processed':email_Processed, 'Info':name_Processed })

        with open('csv_example_input.csv', 'w', newline='', encoding="utf-8") as csvfile:
            fieldnames = ['id','DisplayName','Email', 'DisplayName_Processed','Email_Processed','Info']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames,dialect="excel")
            writer.writeheader()
            for row in rows:
                writer.writerow(row) 
                
# find duplicate pairs of developers
def deDuplication():
    
    input_file = 'csv_example_input.csv'
    output_file = 'csv_example_output.csv'
    settings_file = 'csv_example_learned_settings3'
    training_file = 'csv_example_training.json3'
    def preProcess(column):
    
        try:
            column = column.decode('utf-8')
        except AttributeError:
            pass
        column = unidecode(column)
        column = re.sub(' +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
    
        if not column:
            column = None
        return column
    
    # Read in the data from CSV file:
    def readData(filename):
    
        data_d = {}
        with open(filename, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                clean_row = [(k, preProcess(v)) for (k, v) in row.items()]
                row_id = row['id']
                data_d[row_id] = dict(clean_row)
    
        return data_d
    
    print('importing data ...')
    data_d = readData(input_file)
    
    if os.path.exists(settings_file):
        print('reading from', settings_file)
        with open(settings_file, 'rb') as f:
            deduper = dedupe.StaticDedupe(f)
    else:
        fields = [
            {'field' : 'DisplayName_Processed', 'type': 'String'},
            {'field' : 'Email_Processed', 'type': 'String'},
            {'field' : 'Info', 'type': 'String'},
            ]
        deduper = dedupe.Dedupe(fields)
        deduper.sample(data_d, 15000)
    
        if os.path.exists(training_file):
            print('reading labeled examples from ', training_file)
            with open(training_file, 'rb') as f:
                deduper.readTraining(f)
    
        print('starting active labeling...')
    
        dedupe.consoleLabel(deduper)
    
        deduper.train()
    
        with open(training_file, 'w') as tf:
            deduper.writeTraining(tf)
    
        with open(settings_file, 'wb') as sf:
            deduper.writeSettings(sf)
    
    threshold = deduper.threshold(data_d, recall_weight=1)
    
    print('clustering...')
    clustered_dupes = deduper.match(data_d, threshold)
    
    print('# duplicate sets', len(clustered_dupes))
    
    cluster_membership = {}
    cluster_id = 0
    for (cluster_id, cluster) in enumerate(clustered_dupes):
        id_set, scores = cluster
        cluster_d = [data_d[c] for c in id_set]
        canonical_rep = dedupe.canonicalize(cluster_d)
        for record_id, score in zip(id_set, scores):
            cluster_membership[record_id] = {
                "cluster id" : cluster_id,
                "canonical representation" : canonical_rep,
                "confidence": score
            }
    
    singleton_id = cluster_id + 1
    
    with open(output_file, 'w',encoding="utf-8") as f_output, open(input_file, encoding="utf-8") as f_input:
        writer = csv.writer(f_output)
        reader = csv.reader(f_input)
    
        heading_row = next(reader)
        heading_row.insert(0, 'confidence_score')
        heading_row.insert(0, 'Cluster ID')
        canonical_keys = canonical_rep.keys()
        for key in canonical_keys:
            heading_row.append('canonical_' + key)
    
        writer.writerow(heading_row)
    
        for row in reader:
            row_id = row[0]
            if row_id in cluster_membership:
                cluster_id = cluster_membership[row_id]["cluster id"]
                canonical_rep = cluster_membership[row_id]["canonical representation"]
                row.insert(0, cluster_membership[row_id]['confidence'])
                row.insert(0, cluster_id)
                for key in canonical_keys:
                    row.append(canonical_rep[key].encode('utf8'))
            else:
                row.insert(0, None)
                row.insert(0, singleton_id)
                singleton_id += 1
                for key in canonical_keys:
                    row.append(None)
            writer.writerow(row)
    return clustered_dupes

def generateLinkedDevelopers(duplicatePairs, listOfNamesAndEmail, projectName):
    developersRecord = []
    with open('csv_example_input.csv', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                row_id = row['id']
                row_name = row['DisplayName']
                row_email = row['Email']
                developersRecord.append({'id': row_id,'DisplayName': row_name,'Email':row_email})
    rows = []
    for pair in duplicatePairs:
        #(('2712', '6'), (0.00013264897, 0.00013264897)) pair[0] = ('2712', '6') while  pair[1] = (0.00013264897, 0.00013264897)
        listofdevelopers= pair[0]
        if (len(listofdevelopers)==2 and pair[1][0] >= 0.5):    # if similariy is more than 50%
            firstDeveloper = [item for item in developersRecord if item['id'] == pair[0][0]]
            secondDeveloper = [item for item in developersRecord if item['id'] == pair[0][1]]
            rows.append ({'Project': projectName,'DisplayName':firstDeveloper[0]['DisplayName'],'Email': firstDeveloper[0]['Email'],'Linked_Developer_DisplayName':secondDeveloper[0]['DisplayName'] ,'Linked_Developer_Email':secondDeveloper[0]['Email']})                
        elif (len(listofdevelopers)>2 and pair[1][0] >= 0.5): # if similariy of first pair is more than 50%
            for i in range(1, len(listofdevelopers)):
                firstDeveloper = [item for item in developersRecord if item['id'] == pair[0][0]]
                secondDeveloper = [item for item in developersRecord if item['id'] == pair[0][i]]
                rows.append ({'Project': projectName,'DisplayName':firstDeveloper[0]['DisplayName'],'Email': firstDeveloper[0]['Email'],'Linked_Developer_DisplayName':secondDeveloper[0]['DisplayName'] ,'Linked_Developer_Email':secondDeveloper[0]['Email']})                       
    
  ### find if Jira userid is similar to git email id; store them as duplicates       
        deDuplicatedRows = []    
        for i in range(0,int(len(listOfNamesAndEmail))):
            source_userId = listOfNamesAndEmail[i]['email']
            if "@" in source_userId:
                source_userId = source_userId[:source_userId.find('@')]
            for j in range(i+1,int(len(listOfNamesAndEmail))):
                destination_userId = listOfNamesAndEmail[j]['email']
                if "@" in destination_userId:
                    destination_userId = destination_userId[:destination_userId.find('@')]
                if source_userId == destination_userId:
                    deDuplicatedRows.append ({'Project': projectName,'DisplayName':listOfNamesAndEmail[i]['name'],'Email': listOfNamesAndEmail[i]['email'],'Linked_Developer_DisplayName':listOfNamesAndEmail[j]['name'] ,'Linked_Developer_Email':listOfNamesAndEmail[j]['email']})                
    
    ###### Do not insert any row check if linked_emailId already exist 
    existedEmailIDs = []
    with open('developer_LinkedDevelopers.csv', 'a', newline='', encoding="utf-8") as csvfile:
        fieldnames = ['Project','DisplayName','Email', 'Linked_Developer_DisplayName','Linked_Developer_Email']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames,dialect="excel")
        writer.writeheader()
        for row in rows:
            if row['Linked_Developer_Email'] not in existedEmailIDs: 
                writer.writerow(row)
                existedEmailIDs.append(row['Email'])
        for deduprows in deDuplicatedRows:
            if deduprows['Linked_Developer_Email'] not in existedEmailIDs: 
                writer.writerow(deduprows)
                existedEmailIDs.append(deduprows['Email'])