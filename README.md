# Realtional Dataset
- Downlaod Dump.sql from https://github.com/jku-isse/socio-technical-interaction-dataset/releases
- Import it your local MySQL server MySQL Workbech->Server->Data Import -> Import from Self-Contained File -> Start Import
-

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
