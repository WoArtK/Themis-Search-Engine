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
package gr.csd.uoc.hy463.themis.linkAnalysis.graph.utils;

import gr.csd.uoc.hy463.themis.linkAnalysis.graph.Graph;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 *
 * @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
public class Serialization {

    public static Graph DeserializeGraph(String path) {
        Graph graph = null;
        try {
            FileInputStream fileIn = new FileInputStream(path);
            ObjectInputStream in = new ObjectInputStream(fileIn);
            graph = (Graph) in.readObject();
            in.close();
            fileIn.close();
            System.out.println("Deserialized data from " + path);
            System.out.println("#Nodes of graph:" + graph.getAdjacencyList().size());
        } catch (IndexOutOfBoundsException | IOException | ClassNotFoundException ex) {
            ex.printStackTrace(System.err);
            if (ex instanceof IndexOutOfBoundsException) {
                System.out.println("Please insert path to deserialize graph");
            } else if (ex instanceof FileNotFoundException) {
                System.out.println("Please insert correct path to deserialize graph");
            } else if (ex instanceof IOException) {
                System.out.println("Something went wrong opening file: " + path);
            } else if (ex instanceof ClassNotFoundException) {
                System.out.println("Graph class not found");
            }
            System.exit(-1);
        }

        return graph;
    }

    public static void serializeGraph(Graph graph) {
        try {
            FileOutputStream fileOut
                    = new FileOutputStream("./graph.ser");
            ObjectOutputStream out = new ObjectOutputStream(fileOut);
            out.writeObject(graph);
            out.close();
            fileOut.close();
            System.out.println("Serialized graph is saved in ./graph.ser");
        } catch (IOException i) {
            i.printStackTrace(System.err);
            System.out.println("Graph Serialization failed");
        }
    }

}
