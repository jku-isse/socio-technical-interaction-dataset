# -*- coding: utf-8 -*-
"""
Created on Mon Feb  3 12:08:10 2020

@author: Usman
"""
import networkx as nx
from communityGraphExtraction import comparePairwiseDevArtifactChangeOverlap
from communityGraphExtraction import convertIssueInvolvementToEdges
from communityGraphExtraction import getArtChangeDatesPerDev
from communityGraphExtraction import getMaxIssueInvolvementsPerUser
from communityGraphExtraction import getSubsystems
from communityGraphExtraction import getCrossSubsystemIssueLinks
from communityGraphExtraction import convertCrossSubsystemIssueLinksToEdges
from communityGraphExtraction import getCrossSubsystemDeveloperInvolvement
from communityGraphExtraction import convertSubsystemDeveloperInvolvementToEdges
from cypher_neo4j import graph2Cypher
from string import Template

import matplotlib.pyplot as plt
import sys

from sqlConnection import getDBConn
connection = getDBConn()

def generateDictForProjectWindow(projectId):
    graph = dict()
    overlapWindowInSeconds = 3600*24*30*4 #4months
    changes = getArtChangeDatesPerDev(projectId)
    graph['developerEdgesViaCommits'] = comparePairwiseDevArtifactChangeOverlap(changes, overlapWindowInSeconds)
    involvements = getMaxIssueInvolvementsPerUser(projectId)
    graph['developerEdgesViaIssues'] = convertIssueInvolvementToEdges(involvements)
    graph['developers'] = getDevelopersPerProject(projectId)   
    return graph

def generateDictForSubystems(projectID):
    graph = dict()
    graph['projectID'] = projectID
    (subsys, subsysParent) = getSubsystems(projectID)
    graph['subsystems'] = subsys
    graph['subsystemsParentEdges'] = subsysParent

    links = getCrossSubsystemIssueLinks(projectID)
    graph['subsystemsEdgesViaIssues'] = convertCrossSubsystemIssueLinksToEdges(links)
    subsysInv = getCrossSubsystemDeveloperInvolvement(projectID)
    graph['subsystemEdgesViaInvolvement'] = convertSubsystemDeveloperInvolvementToEdges(subsysInv)
    return graph

def getKeyFromNodes(fromN, toN):
    if fromN > toN:
        return fromN+toN
    else:
        return toN+fromN
    
def getNodeOrder(fromN, toN):
    if fromN > toN:
        return fromN, toN
    else:
        return toN, fromN

def convertDictToGraph(graph):    
    my_graph = nx.Graph()
#    for developer in graph['developers']:
    my_graph.add_nodes_from(graph['developers'])
    allEdges = dict()   
    for edge in graph ['developerEdgesViaCommits']:
        node1 = edge['from']
        node2 = edge['to'] 
        artifact_count = edge['count']
        
        key = getKeyFromNodes(node1, node2)
        node1, node2 = getNodeOrder(node1, node2)  
        allEdges[key]=({'node1':node1,'node2':node2,'commit_intensity':artifact_count,'comment_intensity':0})
    
    for edge in graph ['developerEdgesViaIssues']:
        node1 = edge['from']
        node2 = edge['to']  
        comment_intensity = edge['intensity']
        
        key = getKeyFromNodes(node1, node2)
        node1, node2 = getNodeOrder(node1, node2)
        if key in allEdges:
            allEdges[key].update({'comment_intensity':comment_intensity})
        else:
            allEdges[key]=({'node1':node1,'node2':node2,'commit_intensity':0,'comment_intensity':comment_intensity})
    
    for key, value in allEdges.items():
        weight = (value['commit_intensity'] *5) + value['comment_intensity']
        my_graph.add_edge(value['node1'],value['node2'], weight=weight ) 
      
    return my_graph 

def convertDictToSubsystemsGraph(graph):    
    my_graph = nx.Graph()
    for entery in graph['subsystems']:
        my_graph.add_node(entery['id'])
    allEdges = dict()   
    for edge in graph ['subsystemEdgesViaInvolvement']:
        node1 = edge['from']
        node2 = edge['to'] 
        artifact_count = edge['count']
        
        key = getKeyFromNodes(node1, node2)
        node1, node2 = getNodeOrder(node1, node2)  
        allEdges[key]=({'node1':node1,'node2':node2,'commit_involvement':artifact_count,'comment_involvement':0})
    
    for edge in graph ['subsystemsEdgesViaIssues']:
        node1 = edge['from']
        node2 = edge['to']  
        comment_intensity = edge['count']
        
        key = getKeyFromNodes(node1, node2)
        node1, node2 = getNodeOrder(node1, node2)
        if key in allEdges:
            allEdges[key].update({'comment_intensity':comment_intensity})
        else:
            allEdges[key]=({'node1':node1,'node2':node2,'commit_involvement':0,'comment_involvement':comment_intensity})
    
    for key, value in allEdges.items():
        weight = (value['commit_involvement'] *5) + value['comment_involvement']
        my_graph.add_edge(value['node1'],value['node2'], weight=weight ) 
      
    return my_graph

queryDevelopers = Template('''SELECT d.UserID, d.DisplayName FROM developer d WHERE d.ProjectID = '$proj';''')

def getDevelopersPerProject(projectId):
    cursor = connection.cursor(buffered=True)
    cursor.execute(queryDevelopers.substitute(proj=projectId))
    l = list()
    for (userID) in cursor:
        l.append(userID[0])
    cursor.close()
    return l

def calculateSubsystemDevelopers(projectId):
    comment_Threshold = 5
    commit_Threshold = 2
    cursor = connection.cursor()    
    sql_Query = "SELECT SubsystemID FROM Subsystem WHERE ProjectId = '"+projectId+"'"        
    cursor.execute(sql_Query)  
    subsystems = cursor.fetchall()
    subsystems_dict = dict()
    for subsystem in subsystems:
        subsystems_dict [subsystem[0]] =  generateDictForSubsystemWithIntensivelyInvolvedDevelopers(subsystem, comment_Threshold, commit_Threshold)  
    return subsystems_dict

def generateDictForSubsystemWithIntensivelyInvolvedDevelopers(subsystem, comment_Threshold, commit_Threshold ):
    contributors = subsystemContributorsWithThreshold(subsystem, commit_Threshold)
    commentors = subsystemCommentorsWithThreshold(subsystem, comment_Threshold)   
    listOfDevelopers = contributors + commentors
    listOfDevelopers = list(dict.fromkeys(listOfDevelopers))    
    return listOfDevelopers

    
querySubsystemContributorsWithThreshold = Template('''
SELECT 
    distinct(inv.UserID), Count(inv.UserID) 
FROM
    artifact art 
        INNER JOIN        
    Artifacts_Commits ac ON ac.ArtifactID = art.ArtifactID 
        INNER JOIN
    involvement inv ON ac.CommitID = inv.CommitID
    WHERE art.subsystemID = '$sub_system' 
    Group By inv.UserID; ;''')

def subsystemContributorsWithThreshold(subsystem , threshold):
    cursor = connection.cursor()
    cursor.execute(querySubsystemContributorsWithThreshold.substitute(sub_system=subsystem[0]))
    listOfDevelopers = []
    for (userID, total_involvement) in cursor:
        if total_involvement >= threshold:
            listOfDevelopers.append(userID)
    return listOfDevelopers

querySubsystemCommentorsWithThreshold = Template('''
SELECT 
    distinct(inv.UserID), Count(inv.UserID)
FROM
    Subsystems_Issues si 
        INNER JOIN        
    involvement inv ON inv.IssueID = si.IssueID
    WHERE si.subsystemID = '$sub_system' 
    Group By inv.UserID;''')    

def subsystemCommentorsWithThreshold(subsystem , threshold ):
    cursor = connection.cursor()
    
    cursor.execute(querySubsystemCommentorsWithThreshold.substitute(sub_system=subsystem[0]))
    listOfDevelopers = []
    for (userID, total_involvement) in cursor:
        if total_involvement >= threshold:
            listOfDevelopers.append(userID)
    return listOfDevelopers
    
def generateProjectGraph(projectid):
    graph_dict = generateDictForProjectWindow(projectid)
    my_graph = convertDictToGraph(graph_dict)
    return my_graph

def generateSubsystemsGraph(projectid):
    subsys_graph_dict = generateDictForSubystems(projectid)
    subsys_graph = convertDictToSubsystemsGraph(subsys_graph_dict)
    return subsys_graph


def drawGraph(my_graph):
    plt.figure(3,figsize=(50,50))
    pos=nx.spring_layout(my_graph)
    nx.draw(my_graph, pos,  with_labels= True, font_weight='bold',cmap=plt.cm.Set1, node_size=600 )   



"""Provide Project Name to generate Developer Interaction Graph and Subsystems Graph"""       
projectid = "oodt"

p_graph = generateProjectGraph(projectid)
sys.stdout.write(graph2Cypher(p_graph))

sub_graph = generateSubsystemsGraph(projectid)
sys.stdout.write(graph2Cypher(sub_graph))
        
