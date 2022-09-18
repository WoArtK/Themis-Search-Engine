package gr.csd.uoc.hy463.themis.metrics;

import gr.csd.uoc.hy463.themis.utils.Pair;

import java.util.ArrayList;

public class Query_Data {
    ArrayList<Pair<String,Integer>>  list_of_answers; //pair<returned docid,relevance>
    String query;
    double frequency;
    int qid;

    public String getQuery() {
        return query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public double getFrequency() {
        return frequency;
    }

    public void setFrequency(double frequency) {
        this.frequency = frequency;
    }

    public int getQid() {
        return qid;
    }

    public void setQid(int qid) {
        this.qid = qid;
    }

    public ArrayList<Pair<String, Integer>> getList_of_answers() {
        return list_of_answers;
    }

    public void setList_of_answers(ArrayList<Pair<String, Integer>> list_of_answers) {
        this.list_of_answers = list_of_answers;
    }

    public Query_Data(ArrayList<Pair<String, Integer>> list_of_answers, String query, double frequency, int qid) {
        this.list_of_answers = list_of_answers;
        this.query = query;
        this.frequency = frequency;
        this.qid = qid;
    }

    public Query_Data() {
    }
}
