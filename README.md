titan-tools
===========

titan-tools is a set of tools for repairing inconsistencies in titan cassandra backed graph databases

titan-tools will:

* repair indicies that reference deleted vertices or associate incorrect values with existing vertices
* perform graph wide reindex for a type
* properly remove partially deleted vertices

## Usage

* clone repo and build project
* copy titan-tools-*.jar into titan/lib
* start bin/gremlin.sh
* see below for gremlin shell usage

```groovy
//first, create your graph instance
g = TitanGraphFactory.open('cassandra.properties')

//second, instantiate a TitanGraphTools object
fx = TitanGraphTools.create(g)

//find problems, but don't repair
fx.checkType("type_name")

//find problems, and repair
fx.repairType("type_name")

//perform a graph wide reindex of a type
fx.reindexType("type_name")

//find, but don't delete partially deleted vertices
fx.checkVertices()

//find and delete partially deleted vertices
fx.cleanVertices()
```
