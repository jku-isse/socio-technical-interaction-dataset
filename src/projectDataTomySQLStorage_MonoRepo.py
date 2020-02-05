# -*- coding: utf-8 -*-
"""
Created on Tue Mar  5 12:01:25 2019

@author: Usman
"""

import mysql.connector
import re
import csv
import json
import os
import dedupe
from perceval.backends.core.git import Git
from jira.client import JIRA
from perceval.backends.core.github import GitHub
from future.builtins import next
from unidecode import  unidecode


from sqlConnection import getDBConn
connection = getDBConn()

def fetchAndStoreProjectData(projectName): 
    ########### Git repo details
    repo_url = 'https://github.com/apache/oodt.git'
    repo_dir = '/tmp/oodt.git'
    repo_owner='apache'    
    ########### jira server details
    jira_Project = True
    jiraServer = 'https://issues.apache.org/jira'
        
    commits = getAllGitCommits(repo_url,repo_dir)
    (issues_in_proj, jira) = getAllJiraIssues(projectName,jiraServer )
    issuesAndPullRequests = getAllGitIssues_pullRequests(projectName,repo_owner )
   
    fillProjectTable(projectName)
    fillSubsystemTable(projectName)
    unique_developers = fillDeveloperTable(commits, jira, issues_in_proj, issuesAndPullRequests, projectName, jira_Project)
    
    fillCommitTable(commits, projectName)
    commit_rows = fillInvolvementTable_CommitInvolvement(commits, projectName, unique_developers)
    fillArtifactTable(commits, projectName)
    fillArtifacts_CommitsTable(commits, projectName, commit_rows)
    fillIssueTable_JiraIssues(issues_in_proj, projectName)
    fillInvolvementTable_JiraDevelopers(issues_in_proj, jira, projectName, unique_developers )
    fillIssues_SubsystemsTable(issues_in_proj, projectName)
    fillIssueTable_GitIssues_And_PullRequests(issuesAndPullRequests, projectName, repo_owner)
    
    fillCommits_IssuesTable(issuesAndPullRequests, repo_owner,  projectName, commit_rows)
    fillInvolvemenTable_developerInvolvement(repo_owner,issuesAndPullRequests, projectName,unique_developers )
    fillCommit_IssuesTableAndInvolvementTable_JiraProjects(commits, projectName, commit_rows ) 
    fillCommit_IssuesTableAndInvolvementTable_GitProjects(commits, projectName,commit_rows )
    
    connection.commit()
    connection.close()

##################### fetch all git commits
def getAllGitCommits(repo_url,repo_dir ):   
    repo = Git(uri=repo_url, gitpath=repo_dir)
    commits = [commit for commit in repo.fetch()]
    return commits

######################## fetch all jira issues
def getAllJiraIssues(projectName,jiraServer ):   
    jira_options = {'server':jiraServer}
    """Provide jira user id and password"""
    jira = JIRA(options=jira_options,basic_auth=('', ''))
    projectDetails = 'project='+projectName
    issues_in_proj = jira.search_issues(projectDetails, maxResults= False, fields='issuetype,project,comment,resolution,watches,created,priority,labels,versions,issuelinks,assignee,updated,status,components,description,summary,creator,subtasks,reporter' )
    return issues_in_proj, jira

#################### fetch all git issues and pull requests
def getAllGitIssues_pullRequests(projectName,repo_owner ):  
    """Provide Github api_token from https://github.com/settings/tokens """
    issuesAndPullRequests = GitHub(owner= repo_owner, repository=projectName ,api_token= '',sleep_for_rate=True)
    return issuesAndPullRequests

################### fill project table 
def fillProjectTable(projectName):
    cursor = connection .cursor()
    sql_Query = "INSERT INTO project (ProjectID) VALUES ('"+projectName+"')"
    cursor.execute(sql_Query) 
    cursor.close()
    
################### fill Subsystem table 
def fillSubsystemTable(projectName):
    folder_component = dict()
    totalSubsystem = []
    with open('folder-component.csv') as csvfile:
         reader = csv.DictReader(csvfile)
         for row in reader:
             if row['project'] == projectName:
                 print(row['subsystem'], row['folder'])
                 folder_component.update({row['folder']:row['subsystem']})
                 totalSubsystem.append(row['subsystem'])
                
    cursor = connection .cursor()             
    
    for subsystem in totalSubsystem:             
        linkedFolders =[k for k,v in folder_component.items() if v == subsystem ]
        subSystemURLs = []      
        for folder in linkedFolders:   
            subSystemURLs.append(folder)
        my_json_string = json.dumps(subSystemURLs)
        sql_Query = "INSERT IGNORE INTO subsystem (SubsystemID,ProjectID,Name,SubsystemURLs) VALUES ('"+subsystem+"','"+projectName+"','"+subsystem+"','"+my_json_string+"') ON DUPLICATE KEY UPDATE SubsystemID=SubsystemID"
        cursor.execute(sql_Query)    
    cursor.close()

########################## fill developer table##########    
def fillDeveloperTable(commits, jira, issues_in_proj, issuesAndPullRequests, projectName, jira_Project):  
    cursor = connection .cursor()
    listOfNamesAndEmail = []
    
    #### get developers from commit
    for commit in commits:  
        nameAndEmail = commit["data"]["Author"]
        name= nameAndEmail[:nameAndEmail.index("<")-1]
        email= nameAndEmail[nameAndEmail.find('<')+1:nameAndEmail.find('>')]
        listOfNamesAndEmail.append({'name':name,'email':email})
        
    #### get developers from jira issues   
    if jira_Project == True:     
        for issue in issues_in_proj:
            print(issue.raw['key'])
            comments = []
            comments = issue.raw['fields']['comment']['comments']
            reporterDisplayName = issue.raw['fields']['reporter']['displayName']
            reporterID = issue.raw['fields']['reporter']['name']
            listOfNamesAndEmail.append({'name':reporterDisplayName,'email':reporterID})
            if issue.raw['fields']['assignee'] is not None:
                assigneeDisplayName = issue.raw['fields']['assignee']['displayName']
                #assigneeID = jsonData['fields']['assignee']['name']
                assigneeID = issue.raw['fields']['assignee']['name']
                listOfNamesAndEmail.append({'name':assigneeDisplayName,'email':assigneeID})
            for comment in comments:
                 listOfNamesAndEmail.append({'name':comment['author']['displayName'],'email':comment['author']['name']})
            watcher = jira.watchers(issue.raw['key'])
            for watcher in watcher.watchers:
                listOfNamesAndEmail.append({'name':watcher.raw['displayName'],'email':watcher.raw['name']})
                
    ######## get developers from git issues/pull requests  
    for issue_PullRequest in issuesAndPullRequests.fetch():
        # Adding issue reporter
        userID= issue_PullRequest['data']['user_data']['login']
        displayName = issue_PullRequest['data']['user_data']['name']
        if not displayName:
            displayName =  userID
        listOfNamesAndEmail.append({'name':displayName,'email':userID}) 
        #Adding data of other members of the conversation
        issue_comments = list()
        issue_comments =  issue_PullRequest['data']['comments_data']
        if len(issue_comments) != 0:
            # Adding Conversation starter's data only if ther are more than 1 comments
            userID= issue_PullRequest['data']['user_data']['login']
            displayName = issue_PullRequest['data']['user_data']['name']
            if not displayName:
                displayName =  userID
            listOfNamesAndEmail.append({'name':displayName,'email':userID})
            for issue_comment in issue_comments:
                userID= issue_comment['user_data']['login']
                displayName = issue_comment['user_data']['name']
                if not displayName:
                    displayName =  userID
                listOfNamesAndEmail.append({'name':displayName,'email':userID})
    
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
    return rows

################  fill commit table
def fillCommitTable(commits, projectName):  
    cursor = connection .cursor()    
    for commit in commits: 
        sql_Query = "INSERT INTO commit (CommitID,CommitURL, Date, ProjectID) VALUES (default,'"+commit["data"]["commit"]+"',from_unixtime("+str(commit["updated_on"])+"),'"+projectName+"')"
        cursor.execute(sql_Query)  
        #cursor.execute("INSERT INTO commit VALUES %s, %s, %s, %s, %s", ("default",commit["data"]["commit"],"from_unixtime"+(str(commit["updated_on"])),json.dumps(commit),projectName))           
        sql_Query = "UPDATE commit SET CommitJSONContent = %s WHERE CommitURL = %s"
        val = (str(json.dumps(commit)), commit["data"]["commit"])
        cursor.execute(sql_Query, val)
    cursor.close()
   
########################## fill involvement table##########  
def fillInvolvementTable_CommitInvolvement(commits, projectName, unique_developers):     
    cursor = connection .cursor()   
    sql_Query = "SELECT * FROM commit where ProjectId = '"+projectName+"'"
    cursor.execute(sql_Query) 
    commit_rows = cursor.fetchall()
        
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
        sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type, SourceURL, CommitID, Time) VALUES (default,'"+projectName+"','"+developerID+"','10','"+commit["data"]["commit"]+"','"+str(commitID)+"',from_unixtime("+str(commit["updated_on"])+"))"
        cursor.execute(sql_Query)                
    cursor.close()
    return commit_rows
        
########################## fill artifacts table########## 
def fillArtifactTable(commits, projectName):
    cursor = connection .cursor()
    sql_Query = "SELECT * FROM subsystem WHERE ProjectId = '"+projectName+"'"        
    cursor.execute(sql_Query)  
    subsystem_rows = cursor.fetchall()
    
    folderToComponetMaping = []
    for subsystem_row in subsystem_rows:
        listOfLinkedFolder = subsystem_row[4]
        data  = json.loads(listOfLinkedFolder)
        for value in data:
            folderToComponetMaping.append({'key':subsystem_row[0],'value': value[value.rfind('/')+1:]}) 
    
    for commit in commits: 
        files = commit["data"]["files"]
        for file in files:
            fileURL = file['file']
            if fileURL.rfind("/") != -1:
                fileName = fileURL[fileURL.rfind("/")+1:] 
            else:
                fileName = fileURL
#                fileURL = projectName+"/"+ fileURL
            fileName = fileName.replace("'", '')  
            fileName = fileName.replace(",", '')  
            fileURL = fileURL.replace("'", '')  
            fileURL = fileURL.replace(",", '')  
                
            if (file['file'][:file['file'].find("/")]):
                filePreFix = file['file'][:file['file'].find("/")]
            subsystem = projectName    
            for mapping in folderToComponetMaping:      
                if mapping['value'] == filePreFix:
                    subsystem = mapping['key'] 
            sql_Query = "INSERT IGNORE INTO artifact (ArtifactID,ArtifactURL,SubsystemID,Name,ProjectID) VALUES (default,'"+fileURL+"','"+subsystem+"','"+fileName+"','"+projectName+"') ON DUPLICATE KEY UPDATE ArtifactID=ArtifactID"
            #print("going to insert: "+ fileURL+" "+commit["data"]["commit"])
            cursor.execute(sql_Query)    
    cursor.close()
########################## fill artifacts_Commits table##########  
def fillArtifacts_CommitsTable(commits, projectName, commit_rows):    
    cursor = connection.cursor()
    sql_Query = "SELECT * FROM artifact WHERE ProjectId = '"+projectName+"'"            
    cursor.execute(sql_Query)  
    artifact_rows = cursor.fetchall()     
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
########################## fill issue table##########  
def fillIssueTable_JiraIssues(issues_in_proj, projectName):
    #####jira issues
    cursor = connection .cursor()        
    ############# fill issues first and then linked issues seperately    
    for issue in issues_in_proj:
        print(issue.raw['key'])
        creation_date = issue.raw['fields']['created'][:issue.raw['fields']['created'].find("+")]       
        if issue.raw['fields']['status']['name'] == "Closed":
            close_date = issue.raw['fields']['updated'][:issue.raw['fields']['updated'].find("+")]               
        else:
            close_date = ""
        sql_Query = "INSERT IGNORE INTO issue (IssueID,CreationDate,CloseDate,JiraIssueURL,JiraIssueName, ProjectID) VALUES ('"+issue.raw['key']+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s.000Z'),STR_TO_DATE('"+close_date+"','%Y-%m-%dT%H:%i:%s.000Z'),'"+issue.raw['self']+"','"+issue.raw['key']+"','"+projectName+"') ON DUPLICATE KEY UPDATE IssueID=IssueID"
        cursor.execute(sql_Query) 
        #### store json contents
        sql_Query = "UPDATE issue SET JiraJSONContent = %s WHERE IssueID = %s"
        val = (str(json.dumps(issue.raw)), issue.raw['key'])
        cursor.execute(sql_Query, val)
        #### store linked issues
        linkedIssues = issue.raw['fields']['issuelinks']
        for linkedIssue in linkedIssues:
            label = linkedIssue['type']['inward']
            if 'inwardIssue' in linkedIssue:
                issue_ID =linkedIssue['inwardIssue']['key']
                if issue_ID.startswith( 'OODT-' ):
                    print(issue_ID)
                    sql_Query = "INSERT INTO Linkedissues (ToIssueID,FromIssueID,Label) VALUES ('"+issue.raw['key']+"','"+issue_ID+"','"+label+"')"
                    cursor.execute(sql_Query)   
            if 'outwardIssue' in linkedIssue:
                issue_ID =linkedIssue['outwardIssue']['key']
                if issue_ID.startswith( 'OODT-' ):
                    print(issue_ID)
                    sql_Query = "INSERT INTO Linkedissues (ToIssueID,FromIssueID,Label) VALUES ('"+issue_ID+"','"+issue.raw['key']+"','"+label+"')"
                    cursor.execute(sql_Query)   
    cursor.close()

######################## fill jira developer involvement table
def fillInvolvementTable_JiraDevelopers(issues_in_proj, jira, projectName, unique_developers ):
    cursor = connection .cursor()      
    for issue in issues_in_proj:
        print(issue.raw['key'])
        if issue.raw['key']:
            creation_date = issue.raw['fields']['created'][:issue.raw['fields']['created'].find("+")] 
            comments = []
            comments = issue.raw['fields']['comment']['comments']
            developer_reporter = issue.raw['fields']['reporter']['name']
            for item in unique_developers:
                if item['Linked_Developer_Email'] == developer_reporter:
                   developer_reporter = item['Email']  
            print(developer_reporter)
            sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type, SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_reporter+"','5','"+issue.raw['self']+"','"+issue.raw['key']+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s.000Z'))"
            cursor.execute(sql_Query) 
            
            if issue.raw['fields']['assignee'] is not None:
                developer_assignee= issue.raw['fields']['assignee']['name']
                for item in unique_developers:
                    if item['Linked_Developer_Email'] == developer_assignee:
                       developer_assignee = item['Email']  
                print(developer_assignee)       
                sql_Query = "INSERT INTO involvement (ID,ProjectID,UserID, Type,SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_assignee+"','5','"+issue.raw['self']+"','"+issue.raw['key']+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s.000Z'))"
                cursor.execute(sql_Query)    
                    
            for comment in comments:
                 developer_commenter = comment['author']['name']
                 for item in unique_developers:
                    if item['Linked_Developer_Email'] == developer_commenter:
                        developer_commenter = item['Email']        
                 comment_date = comment['created'][:comment['created'].rfind(".")] 
                 print(developer_commenter) 
                 sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type,SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_commenter+"','5','"+issue.raw['self']+"','"+issue.raw['key']+"',STR_TO_DATE('"+comment_date+"','%Y-%m-%dT%H:%i:%s'))"
                 cursor.execute(sql_Query)
            watcher = jira.watchers(issue.raw['key'])
            for watcher in watcher.watchers:
                developer_watcher = watcher.raw['name']
                for item in unique_developers:
                    if item['Linked_Developer_Email'] == developer_watcher:
                        developer_watcher = item['Email'] 
                print(developer_watcher) 
                sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type,SourceURL, IssueID) VALUES (default,'"+projectName+"','"+developer_watcher+"','1','"+issue.raw['self']+"','"+issue.raw['key']+"')"
                cursor.execute(sql_Query) 
    cursor.close()
     
###################### fill issues_subsystem table
def fillIssues_SubsystemsTable(issues_in_proj, projectName): 
    cursor = connection .cursor()
    sql_Query = "SELECT Name FROM subsystem where ProjectID = '"+projectName+"'"
    cursor.execute(sql_Query)    
    result_set = cursor.fetchall()
    for issue in issues_in_proj:
        components = issue.raw['fields']['components']
        for component in components:
            for result in result_set:
               if component['name'] == result[0]:  
                   sql_Query = "INSERT INTO subsystems_Issues (SubsystemID,IssueID) VALUES ('"+component['name']+"','"+issue.raw['key']+"')"
                   cursor.execute(sql_Query)       
    cursor.close()
    
################################### fill git pull request in issues table
def fillIssueTable_GitIssues_And_PullRequests(issuesAndPullRequests, projectName, repo_owner):            
    cursor = connection .cursor()    
    for item in issuesAndPullRequests.fetch():
        if 'pull_request' in item['data']:
            print(item['data']['number'])
            issue_prefix = repo_owner+'/'+projectName+'#'
            creation_date = item['data']['created_at'][:item['data']['created_at'].find("Z")]  
            if item['data']['closed_at']is not None: 
                close_date = item['data']['closed_at'][:item['data']['closed_at'].find("Z")]
            else:
                close_date = ""
            sql_Query = "INSERT IGNORE INTO issue (IssueID,CreationDate,CloseDate,GitPullRequestURL,GitPullRequestName, ProjectID) VALUES ('"+issue_prefix+str(item['data']['number'])+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s'),STR_TO_DATE('"+close_date+"','%Y-%m-%dT%H:%i:%s'),'"+item['data']['url']+"','"+str(item['data']['number'])+"','"+projectName+"') ON DUPLICATE KEY UPDATE IssueID=IssueID "
            cursor.execute(sql_Query)
        else:
            print(item['data']['number'])
            issue_prefix = repo_owner+'/'+projectName+'#' 
            creation_date = item['data']['created_at'][:item['data']['created_at'].find("Z")]  
            if item['data']['closed_at']is not None: 
                close_date = item['data']['closed_at'][:item['data']['closed_at'].find("Z")]
            else:
                close_date = ""
            sql_Query = "INSERT IGNORE INTO issue (IssueID,CreationDate,CloseDate,GitIssueURL,GitIssueName, ProjectID) VALUES ('"+issue_prefix+str(item['data']['number'])+"',STR_TO_DATE('"+creation_date+"','%Y-%m-%dT%H:%i:%s'),STR_TO_DATE('"+close_date+"','%Y-%m-%dT%H:%i:%s'),'"+item['data']['url']+"','"+str(item['data']['number'])+"','"+projectName+"') ON DUPLICATE KEY UPDATE IssueID=IssueID "
            cursor.execute(sql_Query)
        sql = "UPDATE issue SET GitJSONContent = %s WHERE IssueID = %s"
        val = (str(json.dumps(item)), issue_prefix+str(item['data']['number']))
        cursor.execute(sql, val)            
    cursor.close()
    
#### put data into Commits_Issues table 
def fillCommits_IssuesTable(issuesAndPullRequests, repo_owner,  projectName, commit_rows): 
    cursor = connection .cursor()
    #### pull requests have commits attached to them
    for pull in issuesAndPullRequests.fetch(category='pull_request'):
        data = pull['data']
        title = data['title']
        print(data['number'])
        issue_prefix = repo_owner+'/'+projectName+'#'
        issue_ = issue_prefix+str(data['number'])
        flag = False
        if title.startswith('OODT-'):
            issue_= title[0:9]
            #issue_= message[0:8]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace('[',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")
            flag = True                          
        if title.startswith('[OODT-'):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")   
            flag = True
        if title.startswith('[Oodt-'):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True      
        if title.startswith('[Oodt '):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True        
        if title.startswith('Oodt '):
            issue_= title[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True       
        if title.startswith('Oodt-'):
            issue_= title[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True
        if flag == True:
            if "-" not in issue_:
                issue_ = issue_[:4] + '-' + issue_[4:]
            number = issue_[5:]
            if re.search('[a-zA-Z]', number):
                number = re.sub("\D", "", number)
                issue_= "OODT-"+ number 
            issue_.upper()  
        linkedCommits = data['commits_data']
        for linkedCommit in linkedCommits:     
            for commit_row in commit_rows:
                if commit_row[1] == linkedCommit:
                    print(linkedCommit)
                    print(commit_row[0])
                    print("issueID ="+issue_)
                    ### process pull request text to find a link to Jira issue,then store jira issue id instead of jason pull request
                    sql_Query = "INSERT IGNORE INTO Commits_Issues (CommitID,IssueID) VALUES ('"+str(commit_row[0])+"','"+issue_+"') ON DUPLICATE KEY UPDATE CommitID = CommitID, IssueID = IssueID"
                    cursor.execute(sql_Query)
                    sql = "UPDATE involvement SET IssueId = %s, SourceURL = %s WHERE CommitID = %s and ProjectID = %s"
                    val = (issue_, issue_prefix+str(data['number']),str(commit_row[0]), projectName )
                    cursor.execute(sql, val) 
    cursor.close()
    
##### put data of developer involvemnt in pull request and issues
def fillInvolvemenTable_developerInvolvement(repo_owner,issuesAndPullRequests, projectName, unique_developers):  
    cursor = connection .cursor()    
    for pull_request_issues in issuesAndPullRequests.fetch():
        title = pull_request_issues['data']['title']         
        issue_prefix = repo_owner+'/'+projectName+'#'
        issue_ = issue_prefix + str(pull_request_issues['data']['number'])
        print(pull_request_issues['data']['number'])
        flag = False
        if title.startswith('OODT-'):
            issue_= title[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace('[',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")
            flag = True                            
        if title.startswith('[OODT-'):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")   
            flag = True
        if title.startswith('[Oodt-'):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True   
        if title.startswith('[Oodt '):
            issue_= title[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True       
        if title.startswith('Oodt '):
            issue_= title[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True   
        if title.startswith('Oodt-'):
            issue_= title[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True
        if flag == True:
            if "-" not in issue_:
                issue_ = issue_[:4] + '-' + issue_[4:]
            number = issue_[5:]
            if re.search('[a-zA-Z]', number):
                number = re.sub("\D", "", number)
                issue_= "OODT-"+ number 
            issue_.upper()  
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
            print(issue_)
            sql_Query = "INSERT INTO involvement (ID, ProjectID, UserID, Type,SourceURL, IssueID, Time) VALUES (default,'"+projectName+"','"+developer_pullrequest_commenter+"','5','"+issue_+"','"+issue_+"',STR_TO_DATE('"+comment_date+"','%Y-%m-%dT%H:%i:%s'))"
            cursor.execute(sql_Query)      
    cursor.close()
    
    
####### put Jira project commit data into commit_issues and respectively into involvements    
def fillCommit_IssuesTableAndInvolvementTable_JiraProjects(commits, projectName, commit_rows):    
    cursor = connection .cursor()
    deletedIssues = ['OODT-','Oodt-']
    for commit in commits:
        issue_= ""
        message = commit['data']['message']
        flag = False
        if message.startswith('OODT-'):
            issue_= message[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace('[',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")
            flag = True                           
        if message.startswith('[OODT-'):
            issue_= message[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"")   
            flag = True
        if message.startswith('[Oodt-'):
            issue_= message[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True      
        if message.startswith('[Oodt '):
            issue_= message[1:10]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True      
        if message.startswith('Oodt '):
            issue_= message[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True   
        if message.startswith('Oodt-'):
            issue_= message[0:9]
            issue_= issue_.replace(':',"")
            issue_= issue_.replace(' ',"")
            issue_= issue_.replace(',',"")
            issue_= issue_.replace(']',"")
            issue_= issue_.replace('/',"")
            issue_= issue_.replace('.',"")
            issue_= issue_.replace('\n',"") 
            flag = True
        if flag == True:
            if "-" not in issue_:
                issue_ = issue_[:4] + '-' + issue_[4:]
            number = issue_[5:]
            if re.search('[a-zA-Z]', number):
                number = re.sub("\D", "", number)
                issue_= "OODT-"+ number 
            issue_.upper()
        
        if issue_ != "":
            if issue_ not in deletedIssues:    
                for commit_row in commit_rows:
                    if commit_row[1] == commit['data']['commit']:
                        sql_Query = "INSERT IGNORE INTO commits_Issues (CommitID,IssueID) VALUES ('"+str(commit_row[0])+"','"+issue_+"') ON DUPLICATE KEY UPDATE CommitID = CommitID, IssueID = IssueID "
                        cursor.execute(sql_Query)
                        print("Inserting"+"commit:"+str(commit_row[1])+"issue:"+issue_)
                        sql = "UPDATE involvement SET IssueId = %s WHERE SourceURL = %s and ProjectID = %s"
                        val = (issue_,str(commit_row[1]), projectName )
                        cursor.execute(sql, val) 
    cursor.close()
    
####### put Git porjects commits data into commit_issues and respectively into involvements    
def fillCommit_IssuesTableAndInvolvementTable_GitProjects(commits, projectName,commit_rows ): 
    cursor = connection .cursor()   
    sql_Query = "SELECT * FROM Issue WHERE ProjectId = '"+projectName+"'"        
    cursor.execute(sql_Query)  
    issues_rows = cursor.fetchall()
    
    for git_commit in commits:  
            message = git_commit['data']['message']
            for issue_row in issues_rows:
                issue_number = issue_row[0][issue_row[0].rfind('#')+1:]
                if re.search(r'\b' + ' #'+issue_number + r'\b', message):   
                    for commit_row in commit_rows:
                        if commit_row[1] == git_commit['data']['commit']:
                            sql_Query = "INSERT IGNORE INTO commits_Issues (CommitID,IssueID) VALUES ('"+str(commit_row[0])+"','"+issue_row[0]+"') ON DUPLICATE KEY UPDATE CommitID = CommitID, IssueID = IssueID "
                            cursor.execute(sql_Query)
                            print("I am inserting"+"commit:"+str(commit_row[0])+"issue:"+issue_row[0])
                            sql_Query = "INSERT INTO Involvement (ID, ProjectID,SourceURL, CommitID, IssueID) VALUES (default,'"+projectName+"','"+commit_row[1]+"','"+str(commit_row[0])+"','"+issue_row[0]+"')"
                            cursor.execute(sql_Query)
    cursor.close()
    
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
            

"""Provide Project Name to fetch and store data in realtional database"""

projectName= 'oodt'            
fetchAndStoreProjectData(projectName)            

                

