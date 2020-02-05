# -*- coding: utf-8 -*-
"""
Created on Thu Mar 14 14:34:17 2019

@author: Usman
"""
import mysql.connector
import re
import csv
import json
import os
import dedupe
from perceval.backends.core.git import Git
from perceval.backends.core.github import GitHub
from future.builtins import next
from unidecode import  unidecode
import dateutil.parser
import dateutil.rrule
import dateutil.tz

from sqlConnection import getDBConn
connection = getDBConn()

def fetchAndStoreProjectData(projectName):    
    ########### Git repo details  
    repo_owner= projectName   
    listOfNamesAndEmail = []
    
    fillProjectTable(projectName)
    fillSubsystemTable(projectName, repo_owner, listOfNamesAndEmail)
    unique_developers = fillDeveloperTable(listOfNamesAndEmail,projectName )
    fillInvolvementTable_Dev_CommitInvolvement(projectName,unique_developers)
    fillInvolvemenTable_Dev_IssueInvolvement(repo_owner,projectName, unique_developers)
    #fillLinkedIssuesTable()
    connection.commit()
    connection.close() 
    
################### fill project table 
def fillProjectTable(projectName):
    cursor = connection .cursor()
    sql_Query = "INSERT INTO project (ProjectID) VALUES ('"+projectName+"')"
    cursor.execute(sql_Query) 
    cursor.close()
    
################### fill Subsystem table 
def fillSubsystemTable(projectName, repo_owner, listOfNamesAndEmail):                 
    folder_component = dict()
    totalSubsystem = []
    
    with open('folder-component.csv') as csvfile:
         reader = csv.DictReader(csvfile)
         for row in reader:
             if row['project'] == projectName:
                 print(row['subsystem'], row['folder'])
                 folder_component.update({row['folder']:row['subsystem']})
                 totalSubsystem.append(row['subsystem'])
                
    for subsystem in totalSubsystem:
        connection = getDBConn()
        print("subsystem "+ subsystem)           
        linkedFolders =[k for k,v in folder_component.items() if v == subsystem ]
        subSystemURLs = []      
        for folder in linkedFolders:   
            subSystemURLs.append(folder)
        my_json_string = json.dumps(subSystemURLs)
        repo_url = subSystemURLs[0]+ ".git" 
        cursor = connection .cursor()  
        sql_Query = "INSERT IGNORE INTO subsystem (SubsystemID,ProjectID,Name,SubsystemURLs) VALUES ('"+subsystem+"','"+projectName+"','"+subsystem+"','"+my_json_string+"') ON DUPLICATE KEY UPDATE SubsystemID=SubsystemID"
        cursor.execute(sql_Query)   
        cursor.close()
        connection.commit()
        connection.close
        
        repo_dir = "/tmp/"+subsystem+".git"
        
        commits = getAllGitCommits(repo_url,repo_dir )
        issuesAndPullRequests = getAllGitIssues_pullRequests(repo_owner,subsystem )
        print("i fetched everything fine")
        fillCommitTable(commits, projectName, listOfNamesAndEmail)
        print("fillCommitTable fine")
        fillArtifactTable(commits, projectName, subsystem)
        print("fillArtifactTable fine")
        commit_rows = fillArtifacts_CommitsTable(commits, subsystem, projectName)
        print(len(commit_rows))
        print("fillArtifacts_CommitsTable fine")
        fillIssueTable_GitIssues_And_PullRequests(issuesAndPullRequests, projectName, repo_owner, subsystem, listOfNamesAndEmail)
        print("fillIssueTable_GitIssues_And_PullRequests fine")
        fillCommits_IssuesTable_Through_Issue(issuesAndPullRequests, repo_owner,  projectName, subsystem, commit_rows)
        print("fillCommits_IssuesTable_Through_Issue fine")
        fillCommit_IssuesTable_Through_Commit(commits, projectName, subsystem, commit_rows )
        print("fillCommit_IssuesTable_Through_Commit fine")    

##################### fetch all git commits
def getAllGitCommits(repo_url,repo_dir ):
#    DEFAULT_LAST_DATETIME = datetime.datetime(2019, 11, 19, 0, 0, 0, tzinfo=dateutil.tz.tzutc())
    repo = Git(uri=repo_url, gitpath=repo_dir)
#    commits = [commit for commit in repo.fetch(to_date= DEFAULT_LAST_DATETIME)]
    commits = [commit for commit in repo.fetch()]
    return commits

#################### fetch all git issues and pull requests
def getAllGitIssues_pullRequests(repo_owner, subsystem ):   
    issuesAndPullRequests = GitHub(owner= repo_owner, repository=subsystem ,api_token= 'd5412959f7876d856b8a8bfd6a92a35eb26bb00c',sleep_for_rate=True)
    return issuesAndPullRequests

################  fill commit table
def fillCommitTable(commits, projectName, listOfNamesAndEmail):  
    connection = getDBConn()
    cursor = connection .cursor()    
    for commit in commits: 
        nameAndEmail = commit["data"]["Author"]
        name= nameAndEmail[:nameAndEmail.index("<")-1]
        email= nameAndEmail[nameAndEmail.find('<')+1:nameAndEmail.find('>')]
        listOfNamesAndEmail.append({'name':name,'email':email})
        sql_Query = "INSERT IGNORE INTO commit (CommitID,CommitURL, Date, ProjectID) VALUES (default,'"+commit["data"]["commit"]+"',from_unixtime("+str(commit["updated_on"])+"),'"+projectName+"') ON DUPLICATE KEY UPDATE CommitID=CommitID"
        cursor.execute(sql_Query)  
        sql_Query = "UPDATE commit SET CommitJSONContent = %s WHERE CommitURL = %s"
        val = (str(json.dumps(commit)), commit["data"]["commit"])
        cursor.execute(sql_Query, val)
    cursor.close()
    connection.commit()
    connection.close
        
########################## fill artifacts table########## 
def fillArtifactTable(commits, projectName, subsystem):
    connection = getDBConn()
    cursor = connection .cursor()
    for commit in commits: 
        files = commit["data"]["files"]
        for file in files:
            fileURL = file['file']
            if fileURL.rfind("/") != -1:
                fileName = fileURL[fileURL.rfind("/")+1:] 
            else:
                fileName = fileURL 
            fileName = fileName.replace("'", '')  
            fileName = fileName.replace(",", '')  
            fileURL = fileURL.replace("'", '')  
            fileURL = fileURL.replace(",", '')  
            sql_Query = "INSERT IGNORE INTO artifact (ArtifactID,ArtifactURL,SubsystemID,Name,ProjectID) VALUES (default,'"+fileURL+"','"+subsystem+"','"+fileName+"','"+projectName+"') ON DUPLICATE KEY UPDATE ArtifactID=ArtifactID"
            cursor.execute(sql_Query)    
    cursor.close()
    connection.commit()
    connection.close
########################## fill artifacts_Commits table##########  
def fillArtifacts_CommitsTable(commits, subsystem, projectName):    
    connection = getDBConn()
    cursor = connection.cursor()
    sql_Query = "SELECT * FROM artifact WHERE SubsystemId = '"+subsystem+"'"            
    cursor.execute(sql_Query)  
    artifact_rows = cursor.fetchall()
   
    sql_Query = "SELECT * FROM commit where ProjectId = '"+projectName+"'"
    cursor.execute(sql_Query) 
    commit_rows = cursor.fetchall()
       
    for commit in commits: 
        for commit_row in commit_rows:
            if commit_row[1] == commit["data"]["commit"]:
                commitID = commit_row[0]
        files = commit["data"]["files"]
        for file in files:
            fileURL = file['file']
            artifactID = ""
            for row in artifact_rows:       
                if row[1] == fileURL:
                    artifactID = row[0] 
                    print("Inserting artifact"+str(artifactID))    
                    print("Inserting Commit"+str(commitID))       
                    sql_Query = "INSERT IGNORE INTO artifacts_commits (ArtifactID,CommitID) VALUES ('"+str(artifactID)+"','"+str(commitID)+"') ON DUPLICATE KEY UPDATE ArtifactID=ArtifactID,CommitID = CommitID "
                    #print("going to insert: "+ fileURL+" "+commit["data"]["commit"])
                    cursor.execute(sql_Query)    
    cursor.close()
    connection.commit()
    connection.close
    return commit_rows
     
################################### fill git pull request in issues table
def fillIssueTable_GitIssues_And_PullRequests(issuesAndPullRequests, projectName, repo_owner, subsystem, listOfNamesAndEmail):            
    connection = getDBConn()
    cursor = connection .cursor()
    
    for item in issuesAndPullRequests.fetch():
        if 'pull_request' in item['data']:
            print(item['data']['number'])
            issue_prefix = repo_owner+'/'+subsystem+'#'
            creation_date = item['data']['created_at'][:item['data']['created_at'].find("Z")]  
            if item['data']['closed_at']is not None: 
                close_date = item['data']['closed_at'][:item['data']['closed_at'].find("Z")]
            else:
                close_date = ""
            sql_Query = "INSERT IGNORE INTO issue (IssueID,CreationDate,CloseDate,GitPullRequestURL,GitPullRequestName, ProjectID) VALUES ('"+issue_prefix+str(item['data']['number'])+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s'),STR_TO_DATE('"+close_date+"','%Y-%m-%dT%H:%i:%s'),'"+item['data']['url']+"','"+str(item['data']['number'])+"','"+projectName+"') ON DUPLICATE KEY UPDATE IssueID=IssueID "
            cursor.execute(sql_Query)
            sql_Query = "INSERT IGNORE INTO subsystems_Issues (SubsystemID,IssueID) VALUES ('"+subsystem+"','"+issue_prefix+str(item['data']['number'])+"') ON DUPLICATE KEY UPDATE SubsystemID=SubsystemID, IssueID=IssueID"
            cursor.execute(sql_Query) 
            # Adding issue reporter
            userID= item['data']['user_data']['login']
            displayName = item['data']['user_data']['name']
            if not displayName:
                displayName =  userID
            listOfNamesAndEmail.append({'name':displayName,'email':userID}) 
            #Adding data of other members of the conversation
            issue_comments = list()
            issue_comments =  item['data']['comments_data']
            if len(issue_comments) != 0:
                # Adding Conversation starter's data only if ther are more than 1 comments
                userID= item['data']['user_data']['login']
                displayName = item['data']['user_data']['name']
                if not displayName:
                    displayName =  userID
                listOfNamesAndEmail.append({'name':displayName,'email':userID})
                for issue_comment in issue_comments:
                    userID= issue_comment['user_data']['login']
                    displayName = issue_comment['user_data']['name']
                    if not displayName:
                        displayName =  userID
                    listOfNamesAndEmail.append({'name':displayName,'email':userID})
            
        else:
            print(item['data']['number'])
            issue_prefix = repo_owner+'/'+subsystem+'#' 
            creation_date = item['data']['created_at'][:item['data']['created_at'].find("Z")]  
            if item['data']['closed_at']is not None: 
                close_date = item['data']['closed_at'][:item['data']['closed_at'].find("Z")]
            else:
                close_date = ""
            sql_Query = "INSERT IGNORE INTO issue (IssueID,CreationDate,CloseDate,GitIssueURL,GitIssueName, ProjectID) VALUES ('"+issue_prefix+str(item['data']['number'])+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s'),STR_TO_DATE('"+close_date+"','%Y-%m-%dT%H:%i:%s'),'"+item['data']['url']+"','"+str(item['data']['number'])+"','"+projectName+"') ON DUPLICATE KEY UPDATE IssueID=IssueID "
            cursor.execute(sql_Query)
            sql_Query = "INSERT IGNORE INTO subsystems_Issues (SubsystemID,IssueID) VALUES ('"+subsystem+"','"+issue_prefix+str(item['data']['number'])+"') ON DUPLICATE KEY UPDATE SubsystemID=SubsystemID, IssueID=IssueID"
            cursor.execute(sql_Query) 
            # Adding issue reporter
            userID= item['data']['user_data']['login']
            displayName = item['data']['user_data']['name']
            if not displayName:
                displayName =  userID
            listOfNamesAndEmail.append({'name':displayName,'email':userID})
            
            #Adding data of other members of the conversation
            issue_comments = list()
            issue_comments =  item['data']['comments_data']
            if len(issue_comments) != 0:
                # Adding Conversation starter's data only if ther are more than 1 comments
                userID= item['data']['user_data']['login']
                displayName = item['data']['user_data']['name']
                if not displayName:
                    displayName =  userID
                listOfNamesAndEmail.append({'name':displayName,'email':userID})
                for issue_comment in issue_comments:
                    userID= issue_comment['user_data']['login']
                    displayName = issue_comment['user_data']['name']
                    if not displayName:
                        displayName =  userID
                    listOfNamesAndEmail.append({'name':displayName,'email':userID})
        sql = "UPDATE issue SET GitJSONContent = %s WHERE IssueID = %s"
        val = (str(json.dumps(item)), issue_prefix+str(item['data']['number']))
        cursor.execute(sql, val)
            
    cursor.close()
    connection.commit()
    connection.close
    
#### put data into Commits_Issues table 
def fillCommits_IssuesTable_Through_Issue(issuesAndPullRequests, repo_owner,  projectName, subsystem, commit_rows): 
    connection = getDBConn()
    cursor = connection .cursor()
    #### pull requests have commits attached to them
    for pull in issuesAndPullRequests.fetch(category='pull_request'):
        data = pull['data']
        #title = data['title']
        print(data['number'])
        issue_prefix = repo_owner+'/'+subsystem+'#'
        issue_ = issue_prefix+str(data['number'])
        linkedCommits = data['commits_data']
        for linkedCommit in linkedCommits:
            for commit_row in commit_rows:
                if commit_row[1] == linkedCommit:
                    print(linkedCommit)
                    print(commit_row[0])
                    sql_Query = "INSERT IGNORE INTO Commits_Issues (CommitID,IssueID) VALUES ('"+str(commit_row[0])+"','"+issue_+"') ON DUPLICATE KEY UPDATE CommitID = CommitID, IssueID = IssueID"
                    cursor.execute(sql_Query)
                    sql_Query = "INSERT INTO involvement (ID, ProjectID,Type,SourceURL,CommitID,IssueID,Time) VALUES (default,'"+projectName+"','10','"+issue_+"','"+str(commit_row[0])+"','"+issue_+"','"+str(commit_row[2])+"')"
                    cursor.execute(sql_Query) 
    cursor.close()
    connection.commit()
    connection.close
    
####### put Git porjects commits data into commit_issues and respectively into involvements    
def fillCommit_IssuesTable_Through_Commit(commits, projectName,subsystem,commit_rows ): 
    connection = getDBConn()
    cursor = connection .cursor()
    
    sql_Query = "SELECT Distinct IssueID FROM Subsystems_Issues WHERE SubsystemID = '"+subsystem+"'"        
    cursor.execute(sql_Query)  
    issues_rows = cursor.fetchall()
    
    for git_commit in commits:  
            message = git_commit['data']['message']
            for issue_row in issues_rows:
                issue_number = issue_row[0][issue_row[0].rfind('#')+1:]
                if re.search(r'\b' + ' #'+issue_number + r'\b', message) or  re.search(r'\b'+ ' \(#'+issue_number + r'\b', message): 
                #if re.search(r'\b' + issue_row[0] + r'\b', message):
                #if re.search(r'\b' + ' #'+issue_row[8] + r'\b', comments):  
                    for commit_row in commit_rows:
                        if commit_row[1] == git_commit['data']['commit']:
                            sql_Query = "INSERT IGNORE INTO commits_Issues (CommitID,IssueID) VALUES ('"+str(commit_row[0])+"','"+issue_row[0]+"') ON DUPLICATE KEY UPDATE CommitID = CommitID, IssueID = IssueID "
                            cursor.execute(sql_Query)
                            print("I am inserting"+"commit:"+str(commit_row[0])+"issue:"+issue_row[0])
                            sql_Query = "INSERT INTO Involvement (ID, ProjectID,Type,SourceURL,CommitID,IssueID,Time) VALUES (default,'"+projectName+"','10','"+commit_row[1]+"','"+str(commit_row[0])+"','"+issue_row[0]+"','"+str(commit_row[2])+"')"
                            cursor.execute(sql_Query)
    cursor.close()
    connection.commit()
    connection.close

########################## fill developer table##########    
def fillDeveloperTable(listOfNamesAndEmail, projectName):  
    
    connection = getDBConn()
    cursor = connection .cursor()
    # remove duplicate developers            
    listOfNamesAndEmail = [dict(t) for t in {tuple(d.items()) for d in listOfNamesAndEmail}]           
    # transform developers data to feed de-Duplication algo
    transformation(listOfNamesAndEmail)     
    # run deduplication 
    duplicatePairs = deDuplication()
    # generate developers and linked developers csv
    generateLinkedDevelopers(duplicatePairs, listOfNamesAndEmail, projectName)   
    # store unique developers in DB
    with open('developer_LinkedDevelopers.csv', encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = []
                for row in reader:
                    if row['Project'] == projectName:
                        row_name = row['DisplayName']
                        row_email = row['Email']
                        row_Linked_Developer_Name = row['Linked_Developer_DisplayName']
                        row_Linked_Developer_email = row['Linked_Developer_Email']
                        rows.append({'DisplayName':row_name,'Email':row_email,'Linked_Developer_DisplayName': row_Linked_Developer_Name,'Linked_Developer_Email':row_Linked_Developer_email})
    
    for developer in listOfNamesAndEmail:
        found = False
        for item in rows:
            if item['Linked_Developer_Email'] == developer['email']:
                sql_Query = "INSERT IGNORE INTO developer (UserID,Email,DisplayName,ProjectID) VALUES ('"+item['Email']+"','"+item['Email']+"','"+item['DisplayName']+"','"+projectName+"') ON DUPLICATE KEY UPDATE UserID=UserID"
                cursor.execute(sql_Query)
                found = True         
        if found == False:
            developer['name'] = developer['name'].replace("'",' ')
            sql_Query = "INSERT IGNORE INTO developer (UserID,Email,DisplayName,ProjectID) VALUES ('"+developer['email']+"','"+developer['email']+"','"+developer['name']+"','"+projectName+"') ON DUPLICATE KEY UPDATE UserID=UserID"
            cursor.execute(sql_Query)
            
    cursor.close()
    connection.commit()
    connection.close
    return rows
########################## fill involvement table##########  
def fillInvolvementTable_Dev_CommitInvolvement(projectName, unique_developers):     
    connection = getDBConn()
    cursor = connection .cursor()   
    sql_Query = "SELECT * FROM commit where ProjectId = '"+projectName+"'"
    cursor.execute(sql_Query) 
    commit_rows = cursor.fetchall()
      
    folder_component = dict()
    totalSubsystem = []
    
    with open('folder-component.csv') as csvfile:
         reader = csv.DictReader(csvfile)
         for row in reader:
             if row['project'] == projectName:
                 print(row['subsystem'], row['folder'])
                 folder_component.update({row['folder']:row['subsystem']})
                 totalSubsystem.append(row['subsystem'])
                
    for subsystem in totalSubsystem:
        print("subsystem"+ subsystem)           
        linkedFolders =[k for k,v in folder_component.items() if v == subsystem ]
        subSystemURLs = []      
        for folder in linkedFolders:   
            subSystemURLs.append(folder)
        repo_url = subSystemURLs[0]+ ".git" 
        repo_dir = "/tmp/"+subsystem+".git"        
        commits = getAllGitCommits(repo_url,repo_dir )   
        
        for commit in commits: 
            for commit_row in commit_rows:
                if commit_row[1] == commit["data"]["commit"]:
                    commitID = commit_row[0]
            nameAndEmail = commit["data"]["Author"]
            #name= nameAndEmail[:nameAndEmail.index("<")-1]
            email= nameAndEmail[nameAndEmail.find('<')+1:nameAndEmail.find('>')] 
            developerID = email
            for item in unique_developers:
                if item['Linked_Developer_Email'] == email:
                    #sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type, SourceURL, Time) VALUES (default,'"+projectName+"','"+item['Email']+"','10','"+commit["data"]["commit"]+"',from_unixtime("+str(commit["updated_on"])+"))"
                    developerID =  item['Email'] 
            # check if commit is already present then update user id else insert commit involvement entery
            sql_Query = "SELECT * FROM involvement where ProjectId = '"+projectName+"' AND CommitID = '"+str(commitID)+"'"
            cursor.execute(sql_Query) 
            involvement_rows = cursor.fetchall()
            if len(involvement_rows):
                for inv_row in involvement_rows:
                    sql = "UPDATE involvement SET UserID = %s WHERE ID = %s"
                    val = ( developerID, inv_row[0]  )
                    cursor.execute(sql, val)
                continue
            else:
                sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type, SourceURL, CommitID, Time) VALUES (default,'"+projectName+"','"+developerID+"','10','"+commit["data"]["commit"]+"','"+str(commitID)+"',from_unixtime("+str(commit["updated_on"])+"))"
                cursor.execute(sql_Query)  
         
    cursor.close()
    connection.commit()
    connection.close
   
    
##### put data of developer involvemnt in pull request
def fillInvolvemenTable_Dev_IssueInvolvement(repo_owner, projectName, unique_developers):
    connection = getDBConn()   
    cursor = connection .cursor()   
    
    folder_component = dict()
    totalSubsystem = []
    
    with open('folder-component.csv') as csvfile:
         reader = csv.DictReader(csvfile)
         for row in reader:
             if row['project'] == projectName:
                 print(row['subsystem'], row['folder'])
                 folder_component.update({row['folder']:row['subsystem']})
                 totalSubsystem.append(row['subsystem'])
                
    for subsystem in totalSubsystem:
        print("subsystem"+ subsystem)           
        linkedFolders =[k for k,v in folder_component.items() if v == subsystem ]
        subSystemURLs = []      
        for folder in linkedFolders:   
            subSystemURLs.append(folder)
        issuesAndPullRequests = getAllGitIssues_pullRequests(repo_owner,subsystem )  #from perceval.backends.core.github import GitHub             
        for pull_request_issues in issuesAndPullRequests.fetch():
            issue_prefix = repo_owner+'/'+subsystem+'#'
            issue_ = issue_prefix + str(pull_request_issues['data']['number'])
            print(pull_request_issues['data']['number'])
            # Git reporter = first commenter    
            developer_reporter = pull_request_issues['data']['user_data']['login']
            creation_date = pull_request_issues['data']['created_at'][:pull_request_issues['data']['created_at'].find("Z")] 
            for item in unique_developers:
                if item['Linked_Developer_Email'] == developer_reporter:
                    developer_reporter = item['Email']
            sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type, SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_reporter+"','5','"+issue_+"','"+issue_+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s'))"
            cursor.execute(sql_Query)    
                
            allcomments =  pull_request_issues['data']['comments_data']  
            for comment in allcomments:
                developer_pullrequest_commenter = comment['user_data']['login']
                for item in unique_developers:
                    if item['Linked_Developer_Email'] == developer_pullrequest_commenter:
                        developer_pullrequest_commenter = item['Email']        
                comment_date = comment['created_at'][:comment['created_at'].find("Z")] 
                if comment_date.startswith( '-03-25T',4) :
                    comment_date = comment_date[0:11]+ "14:54:51"
                print(issue_)
                sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type,SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_pullrequest_commenter+"','5','"+issue_+"','"+issue_+"',STR_TO_DATE('"+comment_date+"','%Y-%m-%dT%H:%i:%s'))"
                cursor.execute(sql_Query)      
    cursor.close()
    connection.commit()
    connection.close

#######  linked issues     
def fillLinkedIssuesTable(totalProjectIssues_Pullrequests):
    cursor = connection .cursor()
    for source_Issue in totalProjectIssues_Pullrequests:
        source_Issue_Name = source_Issue['_id']
        source_Issue_Number = source_Issue['data']['number']  
        source_Issue_URL = source_Issue['data']['html_url']
        for target_Issue in totalProjectIssues_Pullrequests:
            comments = getAllComments(target_Issue)
            if re.search(r'\b' + ' #'+str(source_Issue_Number) + r'\b', comments):  
                print(source_Issue_Name,'mentioned in',target_Issue['_id'])
                label = "mentioned-in"
                sql_Query = "INSERT INTO Linkedissues (ToIssueID, FromIssueID,Label) VALUES ('"+source_Issue_Name[19:]+"','"+target_Issue['_id'][19:]+"','"+label+"')"
                cursor.execute(sql_Query)   
    cursor.close()
    
def getAllComments(issue):
    allComments = ""
    allComments+= issue['data']['body']  
    issue_comments = list()
    issue_comments =  issue['data']['comments_data']
    for issue_comment in issue_comments:
        allComments+= issue_comment['body']
    return allComments
    
########################################################################
def transformation(listOfDevelopers):
    rows = []
    counter = 0
    # jira has uniuqe userid while git has unique emailid
    # here name = jira's display name or git name
    # here email = jira userid or git emailid  
    for developer in listOfDevelopers:
        counter += 1 
        name = developer['name']              
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
        else:
            email_Processed = email.replace('.',' ')
            email_Processed = email_Processed.replace('-',' ')
            email_Processed = email_Processed.replace('_',' ')
            email_Processed = ''.join(i for i in email_Processed if not i.isdigit()) 
        
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
            

"""Provide Project Name to fetch and store data in realtional database"""            
projectName= 'networknt'            
fetchAndStoreProjectData(projectName)


  
