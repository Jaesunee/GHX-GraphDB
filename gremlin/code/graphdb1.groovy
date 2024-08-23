def addVertex(graph, inputLabel, id, property = "", value = "") {
    // Check if a vertex with the given label already exists
    def existingVertex = graph.V().hasLabel(inputLabel).has('identifier', id)
    def message = ""

    if (existingVertex.hasNext()) {
        def vertex = existingVertex.next()
        if (property == "" && value == ""){
            message = "The node with label " + inputLabel + " and id " + id + " already exists." 
            println(message)
        } else if (property != "" && value != ""){
            vertex.property(property, value)
            message = "The property " + property + " with value " + value + " was added to the existing node."
            println(message) 
        } else {
            message = "Error: The node exists, but either the property or value are not specified."
            println(message)
        }
    } else {
        if (property == "" && value == ""){
            graph.addV(inputLabel).property('identifier', id).next()
            message = "The node with label " + inputLabel + " and id " + id + " was created." 
            println(message)
        } else if (property != "" && value != ""){
            graph.addV(inputLabel).property('identifier', id).property(property, value).next()
            message = "The node with label " + inputLabel + " and id " + id + " was created with property " + property + " and value " + value 
            println(message)
        } else {
            // Handle the case when either property or value is missing
            message = "Error: The node could not be added because there is a missing property or value."
            println(message)
        }
    }
}

/* old version using custom csvs (like supplier.csv)
def csvToVertices(g, csv, label, property) {
    println "here"
    for (line in csv) {
        if (!line.startsWith("\n")) {
            def lineParts = line.split(',');
            def id = lineParts[0];
            def value = lineParts[1];
            
            addVertex(g, label, id)
            addVertex(g, label, id, property, value)
        }
    }
} 
*/

def csvToVertices(g, csv, label, id) {
    //populating columns
    def first = csv[0].split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)
    def columns = []
    def count = 0
    def id_value = ""
    def id_index = 0

    for (column in first[1..-1]) {
        columns.add(column)
        if(column == id.toUpperCase()){
            id_index = count
        }
        count = count + 1
    }

    //iterate through data, if column matches id, set node id/key to be that value
    for (line in csv[1..-1]) {
        count = 0
        if (line.startsWith("\n")) {
            println("Error: invalid line in csv, skipping")
            continue
        }

        def lineParts = line.split(/,(?=(?:[^"]*"[^"]*")*[^"]*$)/)
        lineParts = lineParts[1..-1]
        id_value = lineParts[id_index]

        if (id_value == ""){
            println("Error: line missing id value, skipping")
            continue
        }else{
            addVertex(g, label, id_value)
        }
        //|| count > columns.size()
        for (part in lineParts){
            if (part == "" || id_index == count){
                count = count + 1
                continue
            }
            addVertex(g, label, id_value, columns[count], part)
            count = count + 1                
        }
    }
}

/* old version of edge creation function
def csvToEdges(g, csv, labelFrom, labelTo, propertyFrom, propertyTo, edgeLabel) {
    println "Creating edges"
    for (line in csv) {
        if (!line.startsWith("\n")) {
            def lineParts = line.split(',')
            def value1 = lineParts[0]
            def value2 = lineParts[1]
            
            // Check if vertices with given IDs exist, and create them if needed
            addVertex(g, labelFrom, value1)
            addVertex(g, labelTo, value2)
            
            // Check if the edge already exists before creating it
            def edgeExists = g.V().hasLabel(labelFrom).has(propertyFrom, value1)
                .outE(edgeLabel)
                .inV().hasLabel(labelTo).has(propertyTo, value2)
                .hasNext()

            if (!edgeExists) {
                // Create an edge between hospital and supplier with a specified label
                g.V().hasLabel(labelFrom).has(propertyFrom, value1)
                    .addE(edgeLabel)
                    .to(__.V().hasLabel(labelTo).has(propertyTo, value2))
                    .iterate()
            }
        }
    }
}
*/

def csvToEdges(g, csv, labelFrom, labelTo, propertyFrom, propertyTo, edgeLabel) {
    println "Creating edges:"
    for (line in csv[1..-1]) {
        if (line.startsWith("\n")) {
            println "Error: no line to read."
        }
        def lineParts = line.split(',')
        def value1 = lineParts[1]
        def value2 = lineParts[2]
        
        // Check if vertices with given IDs exist, and create them if needed
        addVertex(g, labelFrom, value1)
        addVertex(g, labelTo, value2)
        
        // Check if the edge already exists before creating it
        def edgeExists = g.V().hasLabel(labelFrom).has(propertyFrom, value1)
            .outE(edgeLabel)
            .inV().hasLabel(labelTo).has(propertyTo, value2)
            .hasNext()

        if (!edgeExists) {
            // Create an edge between hospital and supplier with a specified label
            g.V().hasLabel(labelFrom).has(propertyFrom, value1)
                .addE(edgeLabel)
                .to(__.V().hasLabel(labelTo).has(propertyTo, value2))
                .iterate()
        }
    }
}



// Create an empty graph
graph = TinkerGraph.open();

// Create the graph traversal source    
g = graph.traversal();

//old data
suppliers = new File('../data/supplier.csv').readLines();
hospitals = new File('../data/hospital.csv').readLines();
supplier2hospital = new File('../data/invoice.csv').readLines();

// using csvs pulled with data pull script
parties = new File('../data/pulled-CSV-Data/party.csv').readLines();
partyInstances = new File('../data/pulled-CSV-Data/party_instance.csv').readLines();
supItems = new File('../data/pulled-CSV-Data/sup_item.csv').readLines();
manItems = new File('../data/pulled-CSV-Data/man_item.csv').readLines();
party2manItems = new File('../data/pulled-CSV-Data/party2manprod.csv').readLines();
party2supItems = new File('../data/pulled-CSV-Data/party2supprod.csv').readLines();
manItems2supItems = new File('../data/pulled-CSV-Data/manprod2suppprod.csv').readLines();

//csvToVertices0(g, suppliers, 'supplier', 'name')
//csvToVertices0(g, hospitals, 'hospital', 'name') 
csvToVertices(g, parties, 'party', 'id')
csvToVertices(g, partyInstances, 'party_instance', 'id')
csvToVertices(g, supItems, 'sup_item', 'id')
csvToVertices(g, manItems, 'man_item', 'id')

// Create edges based on the 'invoice.csv' file
//csvToEdges(g, supplier2hospital, 'supplier', 'hospital', 'identifier', 'identifier', 'supplies')

//create edges based on pulled data (file format is label2label)
csvToEdges(g, manItems2supItems, 'man_item', 'sup_item', 'identifier', 'identifier', 'is')
csvToEdges(g, party2manItems, 'party', 'man_item', 'identifier', 'identifier', 'manufactures')
csvToEdges(g, party2supItems, 'party', 'sup_item', 'identifier', 'identifier', 'supplies')

def printGraphInfo(g) {

    println "\nGraph Summary:"
    
    def numberOfNodes = g.V().count().next()
    println "\nNumber of Nodes: $numberOfNodes"

    def numberOfEdges = g.E().count().next()
    println "Number of Edges: $numberOfEdges"

    def labels = g.V().label().dedup().toList()
    println "Labels: $labels"
    
    def properties = g.V().propertyMap().select(Column.keys).next();
    println "Properties: $properties\n"
}

// Example usage:
printGraphInfo(g)


// Helper function to find a vertex by property
def findVertexByProperty(graph, label, propertyName, propertyValue) {
    return graph.V().has(label, propertyName, propertyValue).tryNext().orElse(null)
}

// Usage example
println "Supplier Vertex: ${findVertexByProperty(g, 'supplier', 'name', 'Staples Corp')}"

// Helper function to find vertices by label
def findVerticesByLabel(graph, label) {
    return graph.V().hasLabel(label).toList()
}

// Usage example
println "All Hospital Vertices: ${findVerticesByLabel(g, 'hospital')}"

// Helper function to count vertices with a certain property
def countVerticesWithProperty(graph, label, propertyName) {
    return graph.V().hasLabel(label).has(propertyName).count().next()
}

// Usage example

println "Number of Supplier Vertices with id: ${countVerticesWithProperty(g, 'supplier', 'identifier')}"

// Helper function to get outgoing edges of a vertex
def getOutgoingEdges(graph, vertex) {
    return graph.V(vertex).outE().toList()
}

// Usage example
println "Supplier's Outgoing Edges: ${getOutgoingEdges(g, findVertexByProperty(g, 'supplier', 'name', 'Staples Corp'))}"

// Helper function to filter edges by label
def getEdgesByLabel(graph, label) {
    return graph.E().hasLabel(label).toList()
}

// Usage example
println "Supplies Edges: ${getEdgesByLabel(g, 'supplies')}"
