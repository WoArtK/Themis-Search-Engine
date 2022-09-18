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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 *  Node representation of graph
 *  @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
public class Node implements java.io.Serializable {

    private static final long serialVersionUID = 1L;

    private String id;
    private int numberOfOutEdges;   // Total number of edges (includes multiple edges)

    public Node(String s) {
        this.id = s;
        this.numberOfOutEdges = 0;
    }

    public void print() {
        System.out.println(id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public boolean equals(Object x) {
        if (x == null || this.id == null) {
            return false;
        }
        // If the object is compared with itself then return true
        if (x == this) {
            return true;
        }
        /* Check if o is an instance of Node or not
          "null instanceof [type]" also returns false */
        if (!(x instanceof Node)) {
            return false;
        }

        // typecast o to Node so that we can compare data members
        Node c = (Node) x;

        // Compare the data members and return accordingly
        return this.id.equals(c.id);
    }

    public void increaseNumberOfOutEdges() {
        this.numberOfOutEdges++;
    }

    public void increaseNumberOfOutEdges(int count) {
        this.numberOfOutEdges = this.numberOfOutEdges + count;
    }

    public void decreaseNumberOfOutEdges() {
        this.numberOfOutEdges--;
    }

    public void decreaseNumberOfOutEdges(Integer num) {
        this.numberOfOutEdges = this.numberOfOutEdges - num;
    }

    /* Getters and Setters */
    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getNumberOfOutEdges() {
        return this.numberOfOutEdges;
    }

    public void setNumberOfOutEdges(int numberOfOutEdges) {
        this.numberOfOutEdges = numberOfOutEdges;
    }

}
