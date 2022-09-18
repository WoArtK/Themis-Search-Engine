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

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import javafx.geometry.Pos;
import net.sf.extjwnl.JWNLException;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;
import net.sf.extjwnl.data.Synset;
import net.sf.extjwnl.data.Word;
import net.sf.extjwnl.dictionary.Dictionary;

import java.io.FileNotFoundException;
import java.util.List;

/**
 * Class that showcases example of external java wordnet library usage
 *
 * @author Panagiotis Papadakos (papadako@ics.forth.gr)
 */
public class EXTJWNLExample {

    // Get the POS represenation for wordnet

    /**
     * Get the wordnet  Part-of-Speech (POS) representation from the stanford one
     * @param taggedAs
     * @return
     */
    private static POS getPos(String taggedAs) {
        switch(taggedAs) {
            case "NN" :
            case "NNS" :
            case "NNP" :
            case "NNPS" :
                return POS.NOUN;
            case "VB" :
            case "VBD" :
            case "VBG" :
            case "VBN" :
            case "VBP" :
            case "VBZ" :
                return POS.VERB;
            case "JJ" :
            case "JJR" :
            case "JJS" :
                return POS.ADJECTIVE;
            case "RB" :
            case "RBR" :
            case "RBS" :
                return POS.ADVERB;
            default:
                return null;
        }
    }

    public static void main(String[] args) throws FileNotFoundException, JWNLException, CloneNotSupportedException {
        Dictionary dictionary = null;
        if (args.length != 1) {
            dictionary = Dictionary.getDefaultResourceInstance();
        }

        if (null != dictionary) {

            // Find the part-of-speech of query
            // This is the NLP tools from stanford
            MaxentTagger maxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger");
            // This is our query
            String query = "This is an example for information retrieval";
            String taggedQuery = maxentTagger.tagString(query);
            String[] eachTag = taggedQuery.split("\\s+");
            System.out.println("Term      " + "Standford tag");
            System.out.println("---------------------------------- "+eachTag.length);

            for (int i = 0; i < eachTag.length; i++) {
                String term = eachTag[i].split("_")[0];
                String tag = eachTag[i].split("_")[1];
                System.out.println( term + " " + tag);
                POS pos = getPos(tag);

                // Ignore anything that is not a noun, verb, adjective, adverb
                if(pos != null) {
                    System.out.println("pos!=null");
                    // Can get various synsets
                    IndexWord iWord;
                    iWord = dictionary.getIndexWord(pos, term);
                    if(iWord != null) {
                        for (Synset synset : iWord.getSenses()) {
                            List<Word> words = synset.getWords();
                            for (Word word : words) {
                                System.out.println(word.getLemma());
                            }
                        }
                    }
                }
            }
        }
    }
}
