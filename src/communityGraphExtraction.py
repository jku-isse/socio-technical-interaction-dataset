# -*- coding: utf-8 -*-
"""
Created on Thu May 16 12:36:25 2019

@author: Usman
"""
from string import Template
from itertools import combinations
from collections import defaultdict
from collections import deque

from sqlConnection import getDBConn
connection = getDBConn()

query_Developers_withHighInter = Template('''SELECT UserID, COUNT(*) c FROM `c4s.sociotechnicalcongurence`.involvement   where projectId = '$proj' and UserID != "" GROUP BY UserID  HAVING c > 1 ORDER BY c DESC;''')

def getDevelopersWithHighestInteractions(projectId): 
    cursor = connection.cursor()
    cursor.execute(query_Developers_withHighInter.substitute(proj=projectId))
    developers = []
    counter = 0;
    for (UserID) in cursor:
        if (counter<3):
            print(UserID[0])
            developers.append(UserID[0])
            counter += 1
    return developers
 
def sortPair(a, b):
    if (a <= b):
        return a, b
    else:
        return b, a

queryDevelopersPerWindow = Template('''SELECT distinct(UserID) FROM involvement  WHERE ProjectID = '$proj'  and Time <= from_unixtime('$till') and Time >= from_unixtime('$fromTime');''')

def getDevelopersPerWindow(projectId, startDate, tillDate ):
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryDevelopersPerWindow.substitute(proj=projectId, till = tillDate, fromTime = startDate))
    d = list()
    for (userID) in cursor:
        d.append(userID[0])
    cursor.close()
    return d


queryDevelopers = Template('''SELECT d.UserID, d.DisplayName FROM developer d WHERE d.ProjectID = '$proj';''')

def getDevelopersPerProject(projectId):
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryDevelopers.substitute(proj=projectId))
    d = list()
    for (userID, displayName) in cursor:
        #print("{} changed {} on  {:%d %b %Y}".format(
        #            userID, artifactURL, date))
        d.append({'id':userID, 'name':displayName, 'project':None,})
    cursor.close()
    return d

queryArtChange = Template('''
SELECT 
    inv.UserID, ac.ArtifactID, c.Date
FROM
    artifacts_commits ac 
        INNER JOIN        
    commit c ON ac.CommitID = c.CommitID 
        INNER JOIN
    involvement inv ON c.CommitID = inv.CommitID
    WHERE c.ProjectID = '$proj' and UserID != ''
ORDER BY inv.UserID;''')

def getArtChangeDatesPerDev(projectId):
    cursor = connection.cursor()
    cursor.execute(queryArtChange.substitute(proj=projectId))
    d = dict()
    for (userID, artifactURL, date) in cursor:
        #print("{} changed {} on  {:%d %b %Y}".format(
        #            userID, artifactURL, date))
        userD = d.setdefault(userID, defaultdict(list))
        userD[artifactURL].append(date.timestamp())
    cursor.close()
    for art in d.values():
        for value in art.values():
            value.sort()
    return d

def comparePairwiseDevArtifactChangeOverlap(changeDict, overlapWindowInSeconds): 
    edges = list()
    pairs = list(combinations(changeDict.keys(), 2));
    for pair in pairs:
        commonArt = changeDict[pair[0]].keys() & changeDict[pair[1]].keys()
        if (len(commonArt) > 0):
            # print("{} and {} both changed {}".format(pair[0], pair[1], commonArt) )
            # now for the commonArt check which ones are within timefame of XXX
            commonArtDateCounter = 0
            for art in commonArt:
                dateListA = changeDict[pair[0]][art]
                dateListB = changeDict[pair[1]][art]
                if (hasArtifactTimeOverlap(dateListA, dateListB, overlapWindowInSeconds)):
                    commonArtDateCounter += 1
            if (commonArtDateCounter > 0):
                fromN, toN = sortPair(pair[0],pair[1])
                edges.append({'from':fromN, 'to':toN, 'count':commonArtDateCounter}) #we could include the artifact list here
    return edges

def hasArtifactTimeOverlap(dateListA, dateListB, overlapWindow):
     listA = deque(dateListA)
     listB = deque(dateListB)
     while (len(listA) > 0 and len(listB) > 0):
         dist = listA[0] - listB[0]
         if (abs(dist) > overlapWindow): # exceeding distance
             if (dist > 0): # A is later, thus pop B, else pop A
                 listB.popleft()
             else:
                 listA.popleft()
         else:
             return True
     return False
    
# returns for each issue the maximum user involvement by type,
queryCoIssueInvolvement = Template('''SELECT   inv.UserID, MAX(inv.type), inv.IssueID
                            FROM     involvement inv
                            WHERE inv.ProjectID = '$proj' AND inv.IssueID is not null AND inv.UserId != ''
                            GROUP BY inv.UserID, inv.IssueID;''')

def getMaxIssueInvolvementsPerUser(projectId):
    cursor = connection.cursor()
    cursor.execute(queryCoIssueInvolvement.substitute(proj=projectId))
    d = dict()
    for (userID, invType, issueID) in cursor:
        # per user, collect all max user involvements in issues, i.e. dict of dicts
        invD = d.setdefault(userID, dict())
        invD[issueID]=invType
    cursor.close()
    return d   
    
def convertIssueInvolvementToEdges(issueDict):    
    edges = list()
    # per combination of users, check if they have overlapping issues
    pairs = list(combinations(issueDict.keys(), 2));
    for pair in pairs:
        commonIssues = issueDict[pair[0]].keys() & issueDict[pair[1]].keys()
        if (len(commonIssues) > 0):
            # print("{} and {} both involved in {}".format(pair[0], pair[1], commonIssues) )
            # determine sum of involvement
            invTypeSum=0
            for issue in commonIssues:
                minType = min( (int)(issueDict[pair[0]][issue]), (int)(issueDict[pair[1]][issue]))
                invTypeSum += minType
            fromN, toN = sortPair(pair[0],pair[1])
            edges.append({'from':fromN, 'to':toN, 'count':len(commonIssues), 'intensity':invTypeSum, 'issues':list(commonIssues)})
    return edges

queryIssues = Template('''SELECT distinct inv.IssueID FROM involvement inv WHERE inv.ProjectID = '$proj' and inv.Type ='5';''')
queryDevelopers_of_Issues = Template('''SELECT distinct inv.UserID FROM involvement inv WHERE  inv.Type ='5' and inv.IssueID = '$issue';''')

def getIssuesPerProject(projectId):   
    cursor = connection.cursor()
    cursor.execute(queryIssues.substitute(proj=projectId))
    issues = list()
    for (IssueID) in cursor:
        print(IssueID)
        #cursor.execute(queryDevelopers_of_Issues.substitute(issue = IssueID))
        #for (UserID) in cursor:
        issues.append({'id':IssueID})
        #    print(UserID)
    cursor.close()
    return issues

queryArtChange_tillDate = Template('''
SELECT 
    inv.UserID, ac.ArtifactID, c.Date
FROM
    artifacts_commits ac 
        INNER JOIN        
    commit c ON ac.CommitID = c.CommitID 
        INNER JOIN
    involvement inv ON c.CommitID = inv.CommitID
    WHERE c.ProjectID = '$proj' and UserID != '' and inv.Time <= from_unixtime('$till') and inv.Time >= from_unixtime('$fromTime')
ORDER BY inv.UserID;''')

def getArtChangeDatesPerDev_tillDate(projectId, startDate, tillDate, listOfDevelopers):
    """ 
    Returns for each developer from the given list, the list of artifacts 
    changed by that developer within the provided time range
        
    Returns
    -------
    dictionary of userId as key and dictionary of date lists as value, 
    where inner dictionary has artifactURL as key and artifact change dates in a list        
    """
    cursor = connection.cursor()
    cursor.execute(queryArtChange_tillDate.substitute(proj=projectId, till = tillDate, fromTime = startDate))
    d = dict()
    for (userID, artifactURL, date) in cursor:
        #print("{} changed {} on  {:%d %b %Y}".format(
        #            userID, artifactURL, date))
        #if userID in listOfDevelopers and date.timestamp()<= tillDate and date.timestamp()>=startDate:
        if userID in listOfDevelopers:
            userD = d.setdefault(userID, defaultdict(list))
            userD[artifactURL].append(date.timestamp())
    cursor.close()
    for art in d.values():
        for value in art.values():
            value.sort()
    return d

# returns for each issue the maximum user involvement by type,
queryCoIssueInvolvement_tillDate = Template('''SELECT   inv.UserID, MAX(inv.type), inv.IssueID, inv.Time
                            FROM     involvement inv
                            WHERE inv.ProjectID = '$proj' AND inv.IssueID is not null AND inv.UserId != '' and inv.Time <= from_unixtime('$till') and inv.Time >= from_unixtime('$fromTime')
                            GROUP BY inv.UserID, inv.IssueID;''')

def getMaxIssueInvolvementsPerUser_tillDate(projectId, startDate, tillDate,listOfDevelopers ):
    """ 
    Returns the highest involvement in an issue per developer for the given 
    list of developers within the provided time range
        
    Returns
    -------
    dictionary of userId as key and dictionary as value, 
    where inner dictionary has issueId as key and involvement type as int value        
    """

    cursor = connection.cursor()
    cursor.execute(queryCoIssueInvolvement_tillDate.substitute(proj=projectId, till = tillDate, fromTime = startDate))
    d = dict()
    for (userID, invType, issueID, Time) in cursor:
        # per user, collect all max user involvements in issues, i.e. dict of dicts
        if userID in listOfDevelopers:
            invD = d.setdefault(userID, dict())
            invD[issueID]=invType
    cursor.close()
    return d 

queryProjectStartTime = Template('''SELECT Min(inv.Time) FROM involvement inv WHERE inv.ProjectID = '$proj' ;''')
queryProjectEndTime = Template('''SELECT Max(inv.Time) FROM involvement inv WHERE inv.ProjectID = '$proj' ;''')

def getProjectDates(projectId):
    projectStartTime = 0
    projectEndTime = 0
    cursor = connection.cursor()
    cursor.execute(queryProjectStartTime.substitute(proj=projectId)) 
    for (start_date) in cursor:
     #print(start_date)
         projectStartTime = start_date[0].timestamp()
    cursor.close()
 
    cursor = connection.cursor()
    cursor.execute(queryProjectEndTime.substitute(proj=projectId)) 
    for (end_date) in cursor:
    #print(start_date)
        projectEndTime = end_date[0].timestamp()
    return projectStartTime,projectEndTime

queryCommitsPerInterval = Template('''SELECT distinct CommitID FROM involvement WHERE ProjectID = '$proj'
                    and  Time >= from_unixtime('$fromTime') and Time <= from_unixtime('$till') 
                    and CommitID is not null;''')
def getCommitsPerInterval(projectId, startDate, tillDate):
    cursor = connection.cursor()
    cursor.execute(queryCommitsPerInterval.substitute(proj=projectId, fromTime = startDate, till = tillDate))
    cursor.fetchall()
    return cursor.rowcount 

queryActiveIssuesPerInterval = Template('''SELECT distinct IssueID FROM involvement WHERE ProjectID = '$proj'
                    and  Time >= from_unixtime('$fromTime') and Time <= from_unixtime('$till') 
                    and IssueID is not null;''')
def getActiveIssuesPerInterval(projectId, startDate, tillDate):
    cursor = connection.cursor()
    cursor.execute(queryActiveIssuesPerInterval.substitute(proj=projectId, fromTime = startDate, till = tillDate))
    cursor.fetchall()
    return cursor.rowcount 

querySubsystemCommitsPerInterval = Template('''
SELECT 
    distinct(inv.CommitID)
FROM
    artifact art 
        INNER JOIN        
    Artifacts_Commits ac ON ac.ArtifactID = art.ArtifactID 
        INNER JOIN
    involvement inv ON ac.CommitID = inv.CommitID
    WHERE art.subsystemID = '$sub_system'
    and  inv.Time >= from_unixtime('$fromTime') and inv.Time <= from_unixtime('$till') 
    and inv.CommitID is not null;''')

def getSubsystemCommitsPerInterval(subsystem, startDate, tillDate):     
    cursor = connection.cursor()
    cursor.execute(querySubsystemCommitsPerInterval.substitute(sub_system=subsystem[0],fromTime = startDate, till = tillDate))
    cursor.fetchall()
    return cursor.rowcount 
        
querySubsystemActiveIssuesPerInterval = Template('''
SELECT 
    distinct(inv.IssueID)
FROM
    Subsystems_Issues si 
        INNER JOIN        
    involvement inv ON inv.IssueID = si.IssueID
    WHERE si.subsystemID = '$sub_system'
    and  inv.Time >= from_unixtime('$fromTime') and inv.Time <= from_unixtime('$till') 
    and inv.IssueID is not null;''')   

def getSubsystemActiveIssuesPerInterval(subsystem, startDate, tillDate ):
    cursor = connection.cursor()
    cursor.execute(querySubsystemActiveIssuesPerInterval.substitute(sub_system=subsystem[0],fromTime = startDate, till = tillDate))
    cursor.fetchall()
    return cursor.rowcount 


queryContributorsPerInterval = Template('''
SELECT distinct(UserID), Count(UserID)
    FROM involvement where   type = "10" and projectid = "ambari"
    and Time >= from_unixtime('$fromTime') and Time <= from_unixtime('$till')  
    Group By UserID;''')  

def getContributorsPerInterval(startDate, tillDate ):
    cursor = connection.cursor()
    cursor.execute(queryContributorsPerInterval.substitute(fromTime = startDate, till = tillDate))
    developers_CommitCount = dict()
    for (userID, total_involvement) in cursor:
        developers_CommitCount[userID] = total_involvement
    return developers_CommitCount   

queryGetSubsystems = Template("SELECT * FROM subsystem Where subsystem.ProjectID = '$proj';")

queryCrossSubsystemIssueLinks = Template('''SELECT li.ToIssueID, li.FromIssueID, ssi1.SubSystemID, ssi2.SubSystemID FROM linkedissues li
	INNER JOIN issue i1 ON i1.IssueID = li.ToIssueID INNER JOIN subsystems_issues ssi1 ON i1.IssueID = ssi1.IssueID
    INNER JOIN issue i2 ON i2.IssueID = li.FromIssueID INNER JOIN subsystems_issues ssi2 ON i2.IssueID = ssi2.IssueID
    WHERE i2.ProjectID = '$proj' AND ssi1.SubsystemID <> ssi2.SubsystemID ;''')

def getSubsystems(projectId):
    cursor = connection.cursor()
    cursor.execute(queryGetSubsystems.substitute(proj=projectId))
    subsys = list()
    subsysParent = list()
    for (subsysID, projID, name, parent, subsysURLs) in cursor:
        subsys.append({'id':subsysID, 'project':projID, 'name':name})
        if (parent != None):
            subsysParent.append({'child':subsysID, 'parent':parent})    
    cursor.close()
    return subsys, subsysParent 

def getCrossSubsystemIssueLinks(projectId):
    cursor = connection.cursor()
    cursor.execute(queryCrossSubsystemIssueLinks.substitute(proj=projectId))
    links = list()
    for (fromIssue, toIssue, fromSubsystem, toSubsystem) in cursor:
        links.append({'fromIssue':fromIssue, 'toIssue':toIssue, 'fromSubsystem':fromSubsystem, 'toSubsystem':toSubsystem})
    cursor.close()
    return links 

def convertCrossSubsystemIssueLinksToEdges(links):
    edges = list()
    subsPairs = defaultdict(list)
    for link in links:
        # collect all links per subsystem pair:
        if (link['fromSubsystem'] > link['toSubsystem']):
            subsPairs[(link['fromSubsystem'],link['toSubsystem'])].append(link)
        else:    
            subsPairs[(link['toSubsystem'],link['fromSubsystem'])].append(link)
    #print(subsPairs)
    for pair in subsPairs.keys():
        issueSet = set()
        for link in subsPairs[pair]:
            issueSet.add(link['fromIssue'])
            issueSet.add(link['toIssue'])
        fromN, toN = sortPair(pair[0],pair[1])
        edges.append({'from':fromN, 'to':toN, 'count':len(subsPairs[pair]), 'issues':list(issueSet)}) # need to check how to add a list to neo4j edge   
    return edges


queryMaxDeveloperInvolvementPerSubsystem = Template('''SELECT inv.UserID, MAX(inv.type), ssi.SubsystemID FROM involvement inv 
                                            	INNER JOIN issue i ON i.IssueID = inv.IssueID 
                                             INNER JOIN subsystems_issues ssi ON ssi.IssueID = i.IssueID
                                             WHERE inv.ProjectID = '$proj' AND inv.type > 1
                                             GROUP BY inv.UserID, ssi.SubsystemID; ''')

def getCrossSubsystemDeveloperInvolvement(projectId):
    cursor = connection.cursor()
    cursor.execute(queryMaxDeveloperInvolvementPerSubsystem.substitute(proj=projectId))
    d = defaultdict(set)
    for (userId, invType, subsystemID) in cursor:
        d[subsystemID].add(userId) # we dont need the type
    cursor.close()
    return d 
    
def convertSubsystemDeveloperInvolvementToEdges(subSysDict):
    edges = list()
    # per combination of users, check if they have overlapping issues
    pairs = list(combinations(subSysDict.keys(), 2));
    for pair in pairs:
        commonDevs = subSysDict[pair[0]].intersection(subSysDict[pair[1]]) 
        if (len(commonDevs) > 0):
            # print("{} and {} both involved in {}".format(pair[0], pair[1], commonIssues) )
            fromN, toN = sortPair(pair[0],pair[1])
            edges.append({'from':fromN, 'to':toN, 'count':len(commonDevs), 'developers':list(commonDevs)})
    return edges
























