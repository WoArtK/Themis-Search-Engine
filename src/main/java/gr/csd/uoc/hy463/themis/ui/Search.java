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
package gr.csd.uoc.hy463.themis.ui;

import gr.csd.uoc.hy463.themis.indexer.Indexer;
import gr.csd.uoc.hy463.themis.indexer.indexes.Index;
import gr.csd.uoc.hy463.themis.indexer.model.DocInfoEssential;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.stemmer.Stemmer;
import gr.csd.uoc.hy463.themis.metrics.MetricValues;
import gr.csd.uoc.hy463.themis.metrics.Query_Data;
import gr.csd.uoc.hy463.themis.metrics.themisEval;
import gr.csd.uoc.hy463.themis.queryExpansion.EXTJWNL;
import gr.csd.uoc.hy463.themis.queryExpansion.Glove;
import gr.csd.uoc.hy463.themis.retrieval.QueryTerm;
import gr.csd.uoc.hy463.themis.retrieval.models.ARetrievalModel;
import gr.csd.uoc.hy463.themis.retrieval.models.Existential;
import gr.csd.uoc.hy463.themis.retrieval.models.OkapiBM25;
import gr.csd.uoc.hy463.themis.retrieval.models.VSM;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.stemmer.StopWords;
import gr.csd.uoc.hy463.themis.utils.Pair;
import net.sf.extjwnl.JWNLException;
import org.javatuples.Triplet;
import org.json.simple.parser.ParseException;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * Some kind of simple ui to search the indexes. Some kind of GUI will be a
 * bonus!
 *
 * @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
public class Search {
    public static void main(String[] args) throws IOException, ClassNotFoundException, ParseException, JWNLException {
        Indexer indexer = new Indexer();
        Stemmer.Initialize();
        StopWords.Initialize();
        String searchmodel;
        String topk = "-";

        themisEval themiseval = new themisEval();
        themiseval.readQuriesJson();
        String[] queries = new String[themiseval._listOfEvaluatedQueries_.size()];
        for (int i = 0; i < queries.length; i++) {
            queries[i] = themiseval._listOfEvaluatedQueries_.get(i).getQuery().replaceAll("[^a-zA-Z0-9]", " ").toLowerCase();
        }

        System.out.println("start of words expanssion");
        String[] extendedQueries = EXTJWNL.extendQuery(queries);
        System.out.println("end of words expanssion");
        for (int b = 0; b <= 1; b++) {

//        for(Query_Data q:themiseval._listOfEvaluatedQueries_){
//            System.out.println(q.getQuery() +"-"+q.getQid()+"-"+q.getFrequency());
//            for(Pair<String,Integer> p:q.getList_of_answers()){
//                System.out.println(p.getL()+"-"+p.getR());
//            }
//        }

//        if (!searchmodel.equals("existential")) {
//            while (true) {
//                System.out.println("type number of retrieved documents(\"-\" for the whole list)");
//                Scanner in = new Scanner(System.in);
//                topk = in.nextLine().toLowerCase();
//                if (topk.equals("-"))
//                    break;
//                else if (topk.replaceAll("[^0-9]", "").equals("")) {
//
//                } else {
//                    break;
//                }
//            }
//        }
//        if (searchmodel.equals("existential")) {
//            Existential existential = new Existential(indexer);
//            while (true) {
//                List<QueryTerm> query = new LinkedList<>();
//                System.out.println("hey.ask me something :) (type exit to exit)");
//                Scanner in = new Scanner(System.in);
//                String s = (in.nextLine()).replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();
//                if (s.equals("exit")) break;
//                StringTokenizer tokens = new StringTokenizer(s);
//                HashMap<String, Double> termMap = new HashMap<>();
//                double maxfreq = 1;
//                while (tokens.hasMoreTokens()) {
//                    String term = tokens.nextToken();
//
//                    if (!StopWords.isStopWord(term)) {
//                        term = Stemmer.Stem(term);
//
//                        if (termMap.containsKey(term)) {
//                            double tf = termMap.get(term);
//                            tf++;
//                            termMap.put(term, tf);
//                            if (tf > maxfreq) {
//                                maxfreq = tf;
//                            }
//                        } else {
//                            termMap.put(term, (double) 1);
//                        }
//                        //query.add(new QueryTerm(term));
//                    }
//                }
//                for (String term : termMap.keySet()) {
//                    double tf = termMap.get(term);
//                    tf = tf / maxfreq;
//                    System.out.println(term + "-" + tf);
//                    query.add(new QueryTerm(term, tf));// oxi to weigth autp tha upolistei sth vm mesa pou uparxei
//                    // to N apo to infos.doc (edw den eixe access sto path tou) gia na upologisei to idf
//                }
//
//
//                //System.out.println("mphke swsta");
//                List<Pair<Object, Double>> returnList = existential.getRankedResults(query, ARetrievalModel.RESULT_TYPE.PLAIN);
//                for (Pair<Object, Double> pair : returnList) {
//                    String obj = pair.getL().toString();
//                    System.out.println(obj);
//                }
//            }
//

//        } else if (searchmodel.equals("vsm")) {
            if (b == 0) {

                long startTime = System.nanoTime();
                VSM vsm = new VSM(indexer);
                ArrayList<Pair<String, ArrayList<String>>> QueriesAnswers = new ArrayList<>();


                for (int i = 0; i < extendedQueries.length; i++) {
                    System.out.println("before: " + queries[i] + "\nafter: " + extendedQueries[i]);
                }

                for (int i = 0; i < extendedQueries.length; i++) {
                    System.out.println("--------------------------------------NEW QUERY------------------------------------------------------------");

                    List<Pair<Object, Double>> returnList;
                    List<QueryTerm> query = new LinkedList<>();
                    String q = "";

                    q = extendedQueries[i];
                    System.out.println(i + ".query after expension in vsm: " + q);
                    StringTokenizer tokens = new StringTokenizer(q);
                    //StringTokenizer tokens = new StringTokenizer(s);
                    HashMap<String, Double> termMap = new HashMap<>(); //krataei kathe term tou query kai to freq wste na upologisoume to tf tou
                    double maxfreq = 1;
                    long starttime = System.nanoTime();
                    while (tokens.hasMoreTokens()) {
                        String term = tokens.nextToken();

                        if (!StopWords.isStopWord(term)) {
                            term = Stemmer.Stem(term);

                            if (termMap.containsKey(term)) {
                                double tf = termMap.get(term);
                                tf++;
                                termMap.put(term, tf);
                                if (tf > maxfreq) {
                                    maxfreq = tf;
                                }
                            } else {
                                termMap.put(term, (double) 1);
                            }
                            //query.add(new QueryTerm(term));
                        }
                    }
                    for (String term : termMap.keySet()) {
                        double tf = termMap.get(term);
                        tf = tf / maxfreq;
//                    System.out.println(term + "-" + tf);
                        query.add(new QueryTerm(term, tf));// oxi to weigth autp tha upolistei sth vm mesa pou uparxei
                        // to N apo to infos.doc (edw den eixe access sto path tou) gia na upologisei to idf
                    }

                    if (topk.equals("-")) {
                        returnList = vsm.getRankedResults(query, ARetrievalModel.RESULT_TYPE.ESSENTIAL);
                    } else {
                        int k = Integer.parseInt(topk);
                        returnList = vsm.getRankedResults(query, ARetrievalModel.RESULT_TYPE.ESSENTIAL, k);

                    }
                    ArrayList<String> answers = new ArrayList<>();// this keeps only the docids of returnList
                    for (Pair<Object, Double> pair : returnList) {
                        answers.add(pair.getL().toString());
//                    System.out.println(pair.getL().toString()+" "+pair.getR());
                    }
                    long endtime = System.nanoTime();
                    long elapsedTime = endtime - starttime;
                    System.out.println("elapsed time: " + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
//                QueriesAnswers.add(new Pair<>(themiseval._listOfEvaluatedQueries_.get(i).getQuery(), answers));
                    themiseval.metricsEvaluation(queries[i], answers, i);

                }
                long endTime = System.nanoTime();
                long time_for_all_queries = endTime - startTime;
                System.out.println("time elapsed for all queries: " + TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS));
                System.out.println("average time per query: " + TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS) / 635);
                MetricValues nDCGValues = themiseval.CalculateValues(themiseval.nDCGs);
                //MetricValues bPrefValues = themiseval.CalculateValues(themiseval.bPrefList);
                MetricValues PrecisionValues = themiseval.CalculateValues(themiseval.Precisions);


                themiseval.write_and_print(PrecisionValues, nDCGValues, "B3_vsm", time_for_all_queries);

                themiseval.nDCGs.clear();

                themiseval.bPrefList.clear();
                themiseval.Precisions.clear();
            } else {


                long startTime = System.nanoTime();
                OkapiBM25 okapiBM25 = new OkapiBM25(indexer);


                for (int i = 0; i < themiseval._listOfEvaluatedQueries_.size(); i++) {
                    System.out.println("--------------------------------------NEW QUERY------------------------------------------------------------");

                    List<QueryTerm> query = new LinkedList<>();

                    String q = "";

                    q = extendedQueries[i];
                    System.out.println(i + ".query after expension in okapi: " + q);
                    StringTokenizer tokens = new StringTokenizer(q); //wait
                    HashMap<String, Double> termMap = new HashMap<>();
                    double maxfreq = 1;
                    long starttime = System.nanoTime();
                    while (tokens.hasMoreTokens()) {
                        String term = tokens.nextToken();

                        if (!StopWords.isStopWord(term)) {
                            term = Stemmer.Stem(term);

                            if (termMap.containsKey(term)) {
                                double tf = termMap.get(term);
                                tf++;
                                termMap.put(term, tf);
                                if (tf > maxfreq) {
                                    maxfreq = tf;
                                }
                            } else {
                                termMap.put(term, (double) 1);
                            }
                            //query.add(new QueryTerm(term));
                        }
                    }
                    for (String term : termMap.keySet()) {
                        double tf = termMap.get(term);
                        tf = tf / maxfreq;
//                    System.out.println(term + "-" + tf);
                        query.add(new QueryTerm(term, tf));// oxi to weigth autp tha upolistei sth vm mesa pou uparxei
                        // to N apo to infos.idx (edw den eixe access sto path tou) gia na upologisei to idf
                    }
                    List<Pair<Object, Double>> returnList = new ArrayList<>();

                    if (topk.equals("-")) {
                        returnList = okapiBM25.getRankedResults(query, ARetrievalModel.RESULT_TYPE.ESSENTIAL);
                    } else {
                        int k = Integer.parseInt(topk);
                        returnList = okapiBM25.getRankedResults(query, ARetrievalModel.RESULT_TYPE.ESSENTIAL, k);
                    }

                    ArrayList<String> answers = new ArrayList<>();// this keeps only the docids of returnList

                    for (Pair<Object, Double> pair : returnList) {
                        //System.out.println(pair.getL().toString() + " " + pair.getR());
                        answers.add(pair.getL().toString());
                    }
                    long endtime = System.nanoTime();
                    long elapsedTime = endtime - starttime;
                    System.out.println("elapsed time: " + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
//                QueriesAnswers.add(new Pair<>(themiseval._listOfEvaluatedQueries_.get(i).getQuery(), answers));
                    themiseval.metricsEvaluation(queries[i], answers, i);
                }
//            long endTime = System.nanoTime();
//            long time_for_all_queries = endTime - startTime;
                long endTime = System.nanoTime();
                long time_for_all_queries = endTime - startTime;
                System.out.println("time elapsed for all queries: " + TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS));
                System.out.println("average time per query: " + TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS) / 635);
                MetricValues nDCGValues = themiseval.CalculateValues(themiseval.nDCGs);
                MetricValues PrecisionValues = themiseval.CalculateValues(themiseval.Precisions);


//                if (b == 3) {
                themiseval.write_and_print(PrecisionValues,  nDCGValues, "B3_okapiBM25", time_for_all_queries);
//            } else {
//                //b==4
//                themiseval.write_and_print(PrecisionValues, bPrefValues, nDCGValues, "B4_okapiBM25", time_for_all_queries);
//
//            }
//            themiseval.write_and_print(PrecisionValues, bPrefValues, nDCGValues, "B3_okapiBM25", time_for_all_queries);
            }
        }
    }

}



