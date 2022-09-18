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
package gr.csd.uoc.hy463.themis.examples;

import gr.csd.uoc.hy463.themis.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;

import java.io.File;
import java.util.Collection;

/**
 * Class that showcases example of Glove usage
 *
 * @author Panagiotis Papadakos (papadako@ics.forth.gr)
 */
public class GloveExample {

    private static final Logger __LOGGER__ = LogManager.getLogger(GloveExample.class);

    public static void main(String[] args) throws Exception {
        Config __CONFIG__ = new Config();  // reads info from themis.config file

        File gloveModel = new File(__CONFIG__.getGloveModelFileName());
        // This will take some time!
        // Wikipedia 2014 + Gigaword 5  from https://nlp.stanford.edu/projects/glove/
        __LOGGER__.info("Loading  model! This will take some time and memory. Please wait...");
        WordVectors model = WordVectorSerializer.readWord2VecModel(gloveModel);

        // Again you can use the stanford pos tagger to identify specific POS to expand...
        // Check the EXTJWNL example

        // For this example just get the two nearest words
        Collection<String> stringList = model.wordsNearest("information", 3);
        System.out.println(stringList);
        stringList = model.wordsNearest("retrieval", 3);
        System.out.println(stringList);
    }
}
