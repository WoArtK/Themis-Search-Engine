/*
 * themis - A fair search engine for scientific articles
 *
 * Currently over the Semantic Scholar Open Research Corpus
 * http://s2-public-api-prod.us-west-2.elasticbeanstalk.com/corpus/
 *
 * Collaborative work with the undergraduate/graduate students of
 * Information Retrieval Systems (hy463) course
 * Spring Semester 2020
 *
 * -- Writing code during COVID-19 pandemic times :-( --
 *
 * Aiming to participate in TREC 2020 Fair Ranking Track
 * https://fair-trec.github.io/
 *
 * Computer Science Department http://www.csd.uoc.gr
 * University of Crete
 * Greece
 *
 * LICENCE: TO BE ADDED
 *
 * Copyright 2020
 *
 */
package gr.csd.uoc.hy463.themis.linkAnalysis.graph;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Graph representation
 *  @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
public class Graph implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    /* holds the nodes and their neighbors along with the number of edges
    to them  (i.e. weight of the connection) */
    private Map<Node, Map<Node, Integer>> adjacencyList;
    private Map<String, Node> publicationCatalogue;     // map from id to node
    private int numOfEdges = 0;

    public Graph() {
        adjacencyList = new HashMap<>();
        publicationCatalogue = new HashMap<>();
    }

    public void addNode(Node node) {
        Object value = adjacencyList.putIfAbsent(node, new HashMap<>());
        // If the vertex was not present add it to the seeds
        if (value == null) {
            // Add it also to the domains catalogue
            publicationCatalogue.put(node.getId(), node);
        }
    }

    /**
     * Get the number of edges of the graph
     * @return
     */
    public int getNumOfEdges() {
        return numOfEdges;
    }

    /**
     * Get the number of nodes for this graph
     * @return
     */
    public int getNumberOfNodes() {
        return publicationCatalogue.size();
    }

    /**
     * Get specific node with id
     * @param id
     * @return
     */
    public Node getNode(String id) {
        return publicationCatalogue.get(id);
    }

    /**
     * Remove node. We do not need this...
     * @param node
     */
    public void removeNode(Node node) {
        // Remove nodes found as neighbor from each entry of the map
        for (Map.Entry<Node, Map<Node, Integer>> entry : adjacencyList.entrySet()) {
            Integer numOfEdges = entry.getValue().remove(node);
            if (numOfEdges != null) {
                entry.getKey().decreaseNumberOfOutEdges(numOfEdges);
            }
        }
        adjacencyList.remove(node);
        publicationCatalogue.remove(node);
    }

    /**
     * Remove node with id
     * @param id
     */
    public void removeNode(String id) {
        Node node = publicationCatalogue.get(id);
        if (node != null) {
            removeNode(id);
        }
    }

    /**
     * Add multiple edges
     *
     * @param from
     * @param to
     * @param count
     */
    public void addEdge(Node from, Node to, int count) {
        adjacencyList.get(from).put(to, count);
        from.increaseNumberOfOutEdges(count);
        numOfEdges = numOfEdges + count;
    }

    /**
     * Add only one edge
     *
     * @param from
     * @param to
     */
    public void addEdge(Node from, Node to) {

        Integer edges = adjacencyList.get(from).get(to);
        if (edges == null) {
            adjacencyList.get(from).put(to, 1);
        } else {
            adjacencyList.get(from).put(to, ++edges);
        }
        numOfEdges++;
        from.increaseNumberOfOutEdges();
    }

    /**
     * Remove an edge
     * @param from
     * @param to
     */
    public void removeEdge(Node from, Node to) {
        Map<Node, Integer> eV1 = adjacencyList.get(from);
        if (eV1 != null) {
            Integer numOfEdges = eV1.remove(to);
            if (numOfEdges != null) {
                from.decreaseNumberOfOutEdges(numOfEdges);
            }
            numOfEdges--;
        }
    }

    /**
     * Get all neighbors
     * @param vertex
     * @return
     */
    public Set<Node> getAdjacents(Node vertex) {
        return adjacencyList.get(vertex).keySet();
    }

    public void print() {
        System.out.println("printing graph");
        for (Map.Entry<Node, Map<Node, Integer>> entry : adjacencyList.entrySet()) {
            entry.getKey().print();
            System.out.println("Out Edges:" + entry.getKey().getNumberOfOutEdges());
            for (Map.Entry<Node, Integer> edge : entry.getValue().entrySet()) {
                System.out.print("\t\t");
                edge.getKey().print();
                System.out.print("\t\t\t Number of edges");
                edge.getValue();
            }
        }
    }

    /**
     * Return the adjacency lists for this map
     * @return
     */
    public Map<Node, Map<Node, Integer>> getAdjacencyList() {
        return this.adjacencyList;
    }

    /**
     * Set the adjacency lists for this graph
     * @param adjacencyList
     */
    public void setAdjacencyList(Map<Node, Map<Node, Integer>> adjacencyList) {
        this.adjacencyList = adjacencyList;
    }

    /**
     * Return the map
     * @return
     */
    public Map<String, Node> getPublicationCatalogue() {
        return this.publicationCatalogue;
    }
}
