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
package gr.csd.uoc.hy463.themis.metrics;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import gr.csd.uoc.hy463.themis.config.Config;
import gr.csd.uoc.hy463.themis.utils.Pair;
import org.apache.logging.log4j.core.util.JsonUtils;
import org.jdom2.output.StAXStreamOutputter;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.json.*;
import org.nd4j.linalg.api.ops.impl.reduce.same.Sum;

import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

public class themisEval {
    public List<Query_Data> _listOfEvaluatedQueries_;
    public List<Double> nDCGs;
    public List<Double> bPrefList;// keeps bpref value for each query
    public List<Double> Precisions; //keeps all precision to find min, max, average and mean
    String path;


    public themisEval() throws IOException {
    }

    //--------------------Read Queries from Json for Evaluation---------------------------
    public void readQuriesJson() throws IOException, ParseException {
        _listOfEvaluatedQueries_ = new LinkedList<>();
        nDCGs = new LinkedList<>();
        bPrefList = new LinkedList<>();
        Precisions = new LinkedList<>();
        Config config = new Config();
        path = config.getIndexPath();
        BufferedReader bufferedReader = new BufferedReader(new FileReader(config.getJudgmentsFileName()));
        String jsonString;
        while ((jsonString = bufferedReader.readLine()) != null) {
            Query_Data query_data = new Query_Data();
            JSONObject obj = new JSONObject(jsonString);
            query_data.setFrequency(obj.getDouble("frequency"));
            query_data.setQid(obj.getInt("qid"));
            query_data.setQuery(obj.getString("query"));
            JSONArray arr = obj.getJSONArray("documents");
            ArrayList<Pair<String, Integer>> list_of_answers = new ArrayList<>();
            for (int p = 0; p < arr.length(); p++) {
                String doc_id = arr.getJSONObject(p).getString("doc_id");
                int relevance = arr.getJSONObject(p).getInt("relevance");
                list_of_answers.add(new Pair<>(doc_id, relevance));
            }
            query_data.setList_of_answers(list_of_answers);
            _listOfEvaluatedQueries_.add(query_data);
        }

    }

    //-------------------- End Read Queries from Json for Evaluation ----------------------


    //--------------------Receives themis answers from Search and evaluates ----------------------------
//    public void metricsEvaluation(ArrayList<Pair<String, ArrayList<String>>> QueriesAnswers) throws IOException {
    //Average Precision
    //nDCG
    //bpref
    public void metricsEvaluation(String query, ArrayList<String> Answers,int i) throws IOException {

        //---------------Average Precision-------------------

        //iterate through queries
        System.out.println("number of answers returned from themis " + Answers.size());

        Query_Data qd = _listOfEvaluatedQueries_.get(i);
        System.out.println("1." + query + "\n2." + qd.query);

        //checks if we work on the same query

        int related = 0; //total number of related docs for this query

        //finds the number of related docs for this query
        for (Pair<String, Integer> tmp : _listOfEvaluatedQueries_.get(i).getList_of_answers()) {
            if (tmp.getR() == 1) {
                related++;
            }
        }
        System.out.println("THEMISEVALUATION. query: " + _listOfEvaluatedQueries_.get(i).getQuery() + ", number or related docs: " + related);

        double Sum = 0;//it refers to the numerator of Average Precision
        double found = 0;//it refers to the number of found docs for the calculation of Precision
        double counter=0;

        for (int j = 0; j < Answers.size(); j++) {
            //we have to iterate the list of given docs for the evaluation of this query
            //and find the common docIds
//                    System.out.println("qd.getList_of_answers().size(): "+qd.getList_of_answers().size());
            for (int k = 0; k < qd.getList_of_answers().size(); k++) {
                if (qd.getList_of_answers().get(k).getR() == 1 && qd.getList_of_answers().get(k).getL().equals(Answers.get(j))) {
                    //its relevance is 1. If it's 0, we skip it.
                    found++;
                    counter++;
                    //System.out.println("found "+found);
//                        System.out.println("P@" + (j + 1) + "=" + P_at_k(j + 1, found));
                    Sum = Sum + P_at_k(counter, found);//k+1 because if k=0 then it's 0 at denominator--wrong
                    break;
                }
                else if(qd.getList_of_answers().get(k).getR() == 0 && qd.getList_of_answers().get(k).getL().equals(Answers.get(j))){
                    counter++;
                    break;
                }

            }

        }
        System.out.println("THEMISEVALUATION. Sum: " + Sum + ", found: " + found);
//            if(Double.isNaN(Sum))   System.err.println("Sum is NaN");
//            if(Double.isNaN(found))   System.err.println("found is NaN");
//            if(found==0.0)  System.err.println("found is 0");
//            if(Sum==0.0)  System.err.println("found is 0");


        Precisions.add(Sum / found);
        System.out.println("THEMISEVALUATION. sum/found: " + (Sum / found) + "Sum: " + Sum + ", found: " + found);

//      todo: add this in Search--  MetricValues PrecisionValues = CalculateValues(Precisions);
        //---------------end of Average Precision-------------------


//        //-------------------bPref----------------------------------
//
//
//        //checks if we work on the same query
//
        related = 0; //total number of related docs for this query
        double irrelevant = 0;
//        //finds the number of related docs for this query
        HashMap<String, Integer> judjed_Documents = new HashMap<>();
        for (Pair<String, Integer> tmp : _listOfEvaluatedQueries_.get(i).getList_of_answers()) {
            judjed_Documents.put(tmp.getL(), tmp.getR());
            if (tmp.getR() == 1) {
                related++;
            } else {
                irrelevant++;
            }
        }
////                System.out.println("query: " + _listOfEvaluatedQueries_.get(i).getQuery() +
////                        ", number or related docs:" + related+
////                        ", number of irrelevant docs:"+irrelevant);
//
//        double denuminator = 1; //use in bpref
//
//        if (related < irrelevant) {
//            denuminator = related;
//        } else {
//            denuminator = irrelevant;
//        }
//
//        double num_irrelevant = 0;//number of irrelevant already retrieved before a relevant retrieved
//        Sum = 0; //it refers to the numinator of bpref
//        //QueriesAnswers.get(i).getR() is the list of answers for this query from our Search Engine
//        for (int j = 0; j < Answers.size(); j++) {
//
//            if (judjed_Documents.containsKey(Answers.get(j))) {
//                //look if the judged Document is relevant or not
//                if (judjed_Documents.get(Answers.get(j)) == 0) {
//                    num_irrelevant++;
//                } else {
//
////                        System.out.println("num_irrelevant "+num_irrelevant);
////                        System.out.println("denuminator "+denuminator);
//                    Sum = Sum + (1 - (num_irrelevant / denuminator));
////                        System.out.println("tha  mpei edw pote arage? + Sum = "+Sum);
//                }
//            }
//        }
//        double bpref = Sum / related;
//        System.out.println("Sum: " + Sum + ", related: " + related);
////            if(Double.isNaN(Sum))   System.err.println("Sum is NaN");
////            if(Double.isNaN(related))   System.err.println("related is NaN");
////            if(related==0.0)  System.err.println("related is 0");
////            if(Sum==0.0)  System.err.println("Sum in bpref is 0");
//
//        bPrefList.add(bpref);
//
//
//        //      todo: add this in Search--   MetricValues bPrefValues = CalculateValues(bPrefList);
//        //-------------------end of bPref---------------------------


        //-------------------start of nDCG---------------------------
        //keeps nDCG inorder to find min, max, average and mean
        //iterate through queries
        //checks if we work on the same query

        related = 0; //total number of related docs for this query
        irrelevant = 0;
        //finds the number of related docs for this query
        judjed_Documents.clear();
        for (Pair<String, Integer> tmp : _listOfEvaluatedQueries_.get(i).getList_of_answers()) {
            judjed_Documents.put(tmp.getL(), tmp.getR());
            if (tmp.getR() == 1) {
                related++;
            } else {
                irrelevant++;
            }
        }

        System.out.println("THEMISEVALUATION. query:" + _listOfEvaluatedQueries_.get(i).getQuery() +
                ", number or related docs:" + related +
                ", number of irrelevant docs:" + irrelevant);


        //------start IDCGp-----------
        double Sum_IDCG = 0;
        for (int j = 0; j < related; j++) {
            double denumerator = Math.log((j + 1) + 1) / Math.log(2);  //(j+1) because of type
            Sum_IDCG = Sum_IDCG + (1 / denumerator);
        }
        //-----end of IDCGp-----------


        //-----start DCGp---------
        double relevant_counter = 0;
        double Sum_DCG = 0;
        for (int j = 0; j < Answers.size(); j++) {
            //if this answer is one of the evaluated and its relevance is 1
            if (judjed_Documents.containsKey(Answers.get(j))
                    && judjed_Documents.get(Answers.get(j)) == 1) {

                relevant_counter++;
                double denumerator = Math.log((relevant_counter+1)) / Math.log(2); //j+1 because of type.iterator starts from 1
                Sum_DCG = Sum_DCG + 1 / denumerator;
                if (relevant_counter == related) break;


            }
            else if(judjed_Documents.containsKey(Answers.get(j))
                    && judjed_Documents.get(Answers.get(j)) == 0){
                relevant_counter++;
            }
        }
        double nDCG = Sum_DCG / Sum_IDCG;
        nDCGs.add(nDCG);


    }

    //      todo: add this in Search--    MetricValues nDCGValues = CalculateValues(nDCGs);
    //-------------------end of nDCG---------------------------


    //calculates max min average and mean
    public MetricValues CalculateValues(List<Double> values) {
        MetricValues metricValues = new MetricValues();
        System.out.println("THEMISEVALUATION. length: "+values.size());
        System.out.println("THEMISEVALUATION. value(0): "+values.get(values.size()-1));
        double min = values.get(0);
        double max = values.get(0);
        double sum = 0;
        double NaN_indicator = 0;
        for (double num : values) {
            if (!Double.isNaN(num)) {
//                if(flag==true){
//                    min = values.get(0);
//                    max = values.get(0);
//                    flag=false;
//                }

                if (num < min) {
                    min = num;
                } else if (num > max) {
                    max = num;
                }
                sum = sum + num;
            } else {
                NaN_indicator++;
                System.err.println("NaN indicator at index: " + NaN_indicator);
            }

        }
        System.out.println("THEMISEVALUATION. CalculateValues Sum: " + sum);
        System.out.println("THEMISEVALUATION. CalculateValues values.size(): " + values.size());

        metricValues.setMax(max);
        metricValues.setMin(min);
        metricValues.setAverage(sum / ((double) values.size() - NaN_indicator));
        metricValues.setMean((max + min) / 2);

        return metricValues;
    }

    private double P_at_k(double k, double found) {
        return found / k;
    }

    public void write_and_print(MetricValues PrecisionValues, MetricValues nDCGValues, String type, long time_for_all_queries) throws IOException {
        System.out.println("-------------"+type+"-------------");
        System.out.println("Average Precision\n");
        System.out.println("Max:" + PrecisionValues.getMax() + "\tMin:" + PrecisionValues.getMin()
                + "\tAverage: " + PrecisionValues.getAverage() + "\tMean: " + PrecisionValues.getMean() + "\n");
        System.out.println("nDCG\n");
        System.out.println("Max:" + nDCGValues.getMax() + "\tMin:" + nDCGValues.getMin()
                + "\tAverage: " + nDCGValues.getAverage() + "\tMean: " + nDCGValues.getMean() + "\n");


        String mypath = path + type + ".idx";
        BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(mypath));
        bufferedWriter.write("\nAverage Precision\n");
        bufferedWriter.write("Max:" + PrecisionValues.getMax() + "\tMin:" + PrecisionValues.getMin()
                + "\tAverage: " + PrecisionValues.getAverage() + "\tMean: " + PrecisionValues.getMean() + "\n");
        bufferedWriter.write("nDCG\n");
        bufferedWriter.write("Max:" + nDCGValues.getMax() + "\tMin:" + nDCGValues.getMin()
                + "\tAverage: " + nDCGValues.getAverage() + "\tMean: " + nDCGValues.getMean() + "\n");

        bufferedWriter.write("Time\n");
        bufferedWriter.write("Time for all queries: " + TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS));
        bufferedWriter.write("\nAverage Time for all queries: " + (TimeUnit.SECONDS.convert(time_for_all_queries, TimeUnit.NANOSECONDS) / 635));
        bufferedWriter.close();

    }


    //--------------------End ->Receives themis answers from Search and evaluates --------------------------------


}
