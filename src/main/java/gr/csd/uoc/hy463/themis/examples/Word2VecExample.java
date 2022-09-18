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
import org.deeplearning4j.models.word2vec.Word2Vec;

import java.io.File;
import java.util.Collection;

/**
 * Class that showcases example of word2vec models, deep learning model from google
 *
 * @author Panagiotis Papadakos (papadako@ics.forth.gr)
 */

public class Word2VecExample {

    private static Logger __LOGGER__ = LogManager.getLogger(Word2VecExample.class);

    public static void main(String[] args) throws Exception {

        Config __CONFIG__ = new Config();  // reads info from themis.config file

        File googleNewsModel = new File(__CONFIG__.getWord2VecModelFileName());
        // This will take some time!
        // Classic model pre-trained Google News corpus (3 billion running words)
        // https://code.google.com/archive/p/word2vec/
        __LOGGER__.info("Loading google model! This will take some time and memory. Please wait...");
        //WordVectors wv = WordVectorSerializer.loadStaticModel(googleNewsModel);
        Word2Vec model = WordVectorSerializer.readWord2VecModel(googleNewsModel, true);

        // For this example just get the two nearest words
        Collection<String> stringList = model.wordsNearest("information", 2);
        System.out.println(stringList);
        stringList = model.wordsNearest("retrieval", 2);
        System.out.println(stringList);
    }
}