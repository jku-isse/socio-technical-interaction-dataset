# Relational Dataset
- Downlaod Dump.sql from https://github.com/jku-isse/socio-technical-interaction-dataset/releases
- Before importing Dump.sql icrease your max_allowed_packet=4M to 16M in your MySQL initialization usually located at C:\ProgramData\MySQL\MySQL Server 8.0\my.ini 
- Import Dump.sql to your local MySQL server by MySQL Workbech->Server->Data Import -> Import from Self-Contained File -> Start Import
 
---------------------------------------------------------------------------------------------------------------------------------------
# Graph Dataset

# Download Neo4j Desktop

Download link can be found here https://neo4j.com/

# Add Graph in Neo4j Desktop

Run Neo4j Desktop application and follow instructions:

- Click on Add Graph
- Set desired name and password
- Click on Create
- Start the Graph database

![GitHub Logo](/AddGraph.PNG)


# Loading Cypher scripts to Graph database

Once the graph database is running, start Neo4j browser and execute cypher script for each project.
as show in the figure below
![GitHub Logo](/browser.png)

The script will insert the data of the graph in the database.

OR

# Loading from graph.db

- Download the graph.db from https://github.com/jku-isse/socio-technical-interaction-dataset/releases
- graph.db contains subfolder DeveloperInteractionGraph.db and SubsystemInteractionGraph.db 
- To visualize any desired graph from the graph.db/DeveloperInteractionGraph.db or SubsystemInteractionGraph.db 
- Create a graph in Neo4j
- Click on Manage
- Click on Open Folder
- Paste the desired (project).db inside the data/databases folder
- Restart DB
- Neo4j credentials:
userid = neo4j
password = admin

## Visualize graph

The resultant graph can be visualized using following query:


Show only 25 nodes
```
$ MATCH (n) RETURN n LIMIT 25
```

Show all nodes
```
$ MATCH (n) RETURN n
```
