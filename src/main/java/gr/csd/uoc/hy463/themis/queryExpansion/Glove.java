package gr.csd.uoc.hy463.themis.queryExpansion;

import gr.csd.uoc.hy463.themis.config.Config;
import gr.csd.uoc.hy463.themis.examples.GloveExample;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.cleartk.opennlp.Tokenizer;
import org.deeplearning4j.models.embeddings.loader.WordVectorSerializer;
import org.deeplearning4j.models.embeddings.wordvectors.WordVectors;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class Glove {

    private static final Logger __LOGGER__ = LogManager.getLogger(GloveExample.class);


    public static String[] extendQuery(String[] query) throws IOException {

        String[] extended = new String[query.length];
        for (int i = 0; i < query.length; i++) {
            extended[i]=query[i];
            HashSet<String> hashSet = new HashSet<>();
            StringTokenizer tokens = new StringTokenizer(query[i]);
            while (tokens.hasMoreTokens()) {
                Config __CONFIG__ = new Config();  // reads info from themis.config file
                String term = tokens.nextToken();
                File gloveModel = new File(__CONFIG__.getGloveModelFileName());
                // This will take some time!
                // Wikipedia 2014 + Gigaword 5  from https://nlp.stanford.edu/projects/glove/
                __LOGGER__.info("Loading  model! This will take some time and memory. Please wait...");
                WordVectors model = WordVectorSerializer.readWord2VecModel(gloveModel);

                // Again you can use the stanford pos tagger to identify specific POS to expand...
                // Check the EXTJWNL example

                // For this example just get the nearest words
                Collection<String> stringList = model.wordsNearest(term, 1);
                for (String str : stringList) {
                    if (!hashSet.contains(str)) {
                        hashSet.add(str);
                    }
                }
            }

            Iterator<String> it = hashSet.iterator();
            while (it.hasNext()) {
                extended[i]+= " "+it.next();;
            }
        }

        return extended;
    }
}
