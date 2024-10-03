# GHX Knowledge Graph Capstone 2023
Healthcare Knowledge Graph Capstone Project

## Description

This repository includes the code and relevant data for the Healthcare Knowledge Graph Capstone Project, created by the senior class of 2024 at the University of Colorado Boulder Computer Science Department, with sponsorship from GHX. The goal of the project was to model the healthcare supply chain using a graph database, allowing people to extract actionable insights through database queries and network analysis algorithms. Additionally, we an interactive UI for querying and visualizing the graph database online can be deployed (neo4j browser).

## Table of Contents

- [Preface](#preface)
- [Installation](#installation)
- [Usage](#usage)
- [Conclusion](#conclusion)

## Preface 

Most of the repository, excluding some general testing scripts data, is only usable with a connection to the GHX network: https://hub.ghx.com/display/DS/EC2+Development+Starter+Kit

Additionally, the code was run on an AWS EC2 instance running linux (ubuntu). Therefore, a lot of the file paths in the code may not be relative, and would need to be changed based on your file structure (or the structure of this repository).

Lastly, this project started off with graph development using gremlin / apache tinkerpop, which correspond to the .groovy files in the gremlin directory. However, development shifted over to neo4j because it seemed more compatible with python, and had a built-in visualization tool (neo4j browser). 

## Installation

In order to run the gremlin or neo4j scripts, they may need to be installed: 
- gremlin installation: https://tinkerpop.apache.org/
- neo4j installation: https://neo4j.com/docs/operations-manual/current/installation/

Also, some python packages like pandas may need to be installed.

## Usage

The repository is organized into different directories:
- chatGHX_API: all code a data related to interacting with ChatGHX, which is an API to consult GHX's trained LLM model
- data_pull_scripts: contains python scripts to pull data from AWS snowflake. pull_data.py is particularly important, because it contains the logic for pulling data from AWS snowflake (SQL) -> pandas dataframes -> CSV or the neo4j graph database itself. Due to limited memory and longer runtimes (or lack of optimization), only a fraction of the data was pulled into the knowledge graph. 
- example_pulled_data: contains some example pulled data. The data was stored in dataframes, and then exported to .csv files. 
- gremlin: contains the code and data used for tinkerpop graph development.
- neo4j: contains the code for adding nodes to the graph from pulled dataframes (adding.py), script to export the neo4j graph to a .graphml file (export.py), and a script to modify the existing neo4j graph. Also contains a sample graph ml file, and some testing scripts.
- network_analysis: contains work around network analysis algorithms, collapsing graphs, etc.

DISCLAIMER: before running any of the neo4j scripts, a neo4j console or server has to be started, possibly in a tmux session (more information in the neo4j installation and setup). 

# Conclusion

There are many improvements and use cases that can be explored:
- Since the knowledge graph is both interactable through graph queries using the online interface, as well as the .graphml file, there is great possibility for further analysis and insight. 
- The code can abstracted to be more manageable and readable, especially pull_data.py.
- The code can be further optimized, especially regarding the process used for pulling relevant data, or batch processing.
- Further work can be done to expand the graph (possibly exploring a method for pulling data to csv, and then populating the graph database solely with long-term storage, and without data in memory, i.e. dataframes), make more relevant relationships between nodes, and additional tools to extract insights from the graph, such as writing specific graph queries or collapsing the graph.
