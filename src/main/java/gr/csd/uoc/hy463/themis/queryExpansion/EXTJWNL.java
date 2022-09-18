package gr.csd.uoc.hy463.themis.queryExpansion;

import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import it.unimi.dsi.fastutil.Hash;
import javafx.geometry.Pos;
import net.sf.extjwnl.JWNLException;
import net.sf.extjwnl.data.IndexWord;
import net.sf.extjwnl.data.POS;
import net.sf.extjwnl.data.Synset;
import net.sf.extjwnl.data.Word;
import net.sf.extjwnl.dictionary.Dictionary;
import org.cleartk.opennlp.Tokenizer;

import java.io.FileNotFoundException;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;


public class EXTJWNL {
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

    public static String[] extendQuery(String[] query) throws JWNLException {
        Dictionary dictionary = Dictionary.getDefaultResourceInstance();

        String[] extended=new String[query.length];
        for(int j=0;j<query.length;j++) {
            extended[j]="";
            HashSet<String> wordsSet=new HashSet<>();
            StringTokenizer tokens = new StringTokenizer(query[j]);
            int counter_of_tokens=0;
            while(tokens.hasMoreTokens()){
                String temp=tokens.nextToken();
                if(!wordsSet.contains(temp)) {
                    wordsSet.add(temp);
                    counter_of_tokens++;
                }
            }
            if (dictionary != null) {
                MaxentTagger maxentTagger = new MaxentTagger("edu/stanford/nlp/models/pos-tagger/english-left3words-distsim.tagger");
                String taggedQuery = maxentTagger.tagString(query[j]);
                String[] eachTag = taggedQuery.split("\\s+");
//            System.out.println("Term      " + "Standford tag");
//            System.out.println("---------------------------------- "+eachTag.length);
                for (int i = 0; i < eachTag.length; i++) {
                    String term = eachTag[i].split("_")[0];
                    String tag = eachTag[i].split("_")[1];
//                System.out.println( term + " " + tag);
                    POS pos = getPos(tag);

                    // Ignore anything that is not a noun, verb, adjective, adverb
                    if (pos != null) {
//                    System.out.println("pos!=null");
                        // Can get various synsets
                        IndexWord iWord;
                        iWord = dictionary.getIndexWord(pos, term);
                        if (iWord != null) {
                            for (Synset synset : iWord.getSenses()) {
                                List<Word> words = synset.getWords();
                                for (Word word : words) {
                                    if (!wordsSet.contains(word.getLemma())) {
                                        StringTokenizer stringTokenizer = new StringTokenizer(word.getLemma());
                                        while(stringTokenizer.hasMoreTokens()){
                                            String temporal=stringTokenizer.nextToken();
                                            if(!wordsSet.contains(temporal)) wordsSet.add(temporal);
                                        }

                                        //s = s + " " + word.getLemma();
                                    }
                                }
                            }
                        }
                    }
                }
            }

            int counter_of_added_words=0;
            for(String word:wordsSet){
                if(counter_of_added_words>counter_of_tokens)    break;
                counter_of_added_words++;
                extended[j]=extended[j]+" "+word;
            }
            extended[j]+=" "+query[j];
        }
        return extended;
    }




}
