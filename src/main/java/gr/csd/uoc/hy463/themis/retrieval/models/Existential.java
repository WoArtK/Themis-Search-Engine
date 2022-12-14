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
package gr.csd.uoc.hy463.themis.retrieval.models;

import gr.csd.uoc.hy463.themis.indexer.Indexer;
import gr.csd.uoc.hy463.themis.indexer.model.DocInfoEssential;
import gr.csd.uoc.hy463.themis.retrieval.QueryTerm;
import gr.csd.uoc.hy463.themis.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

import static gr.csd.uoc.hy463.themis.indexer.Indexer.getDocInfoEssentialForTerms;

/**
 * Implementation of the Existential retrieval model. Returns the documents that
 * contain any of the terms of the query. For this model, there is no ranking of
 * documents, since all documents that have at least one term of the query, are
 * relevant and have a score 1.0
 *
 * @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
public class Existential extends ARetrievalModel {

    public Existential(Indexer index) throws IOException {
        super(index);
        index.load();
    }

    @Override
    public List<Pair<Object, Double>> getRankedResults(List<QueryTerm> query, RESULT_TYPE type) throws IOException {
        List<Pair<Object, Double>> returnList=new LinkedList<>();
        //mexri stigmhs den exei ulopoihthei to Full opote gurnaei to idio me to essential
        if(type==RESULT_TYPE.ESSENTIAL || type==RESULT_TYPE.FULL) {
            List<List<DocInfoEssential>> retrievedList;
            List<String> terms=new ArrayList<>();
            for(QueryTerm q:query){
                terms.add(q.getTerm());
            }
            retrievedList = getDocInfoEssentialForTerms(terms);
            System.out.println(retrievedList.size());
            for (List<DocInfoEssential> ls : retrievedList) {
                for (DocInfoEssential obj : ls) {
                    returnList.add(new Pair<>(obj, 1.0));
                }
            }
        }
        else {
            List<List<DocInfoEssential>> retrievedList;
            List<String> terms=new ArrayList<>();
            for(QueryTerm q:query){
                terms.add(q.getTerm());
            }
            retrievedList = getDocInfoEssentialForTerms(terms);
            System.out.println(retrievedList.size());
            for (List<DocInfoEssential> ls : retrievedList) {
                for (DocInfoEssential obj : ls) {
                    returnList.add(new Pair<>(obj.getId(), 1.0));
                }
            }
        }
        return returnList;

    }

    @Override
    public List<Pair<Object, Double>> getRankedResults(List<QueryTerm> query, RESULT_TYPE type, int topk) throws IOException {
        List<Pair<Object, Double>> returnList=new LinkedList<>();
        //mexri stigmhs den exei ulopoihthei to Full opote gurnaei to idio me to essential
        if(type==RESULT_TYPE.ESSENTIAL || type==RESULT_TYPE.FULL) {
            List<List<DocInfoEssential>> retrievedList;
            List<String> terms=new ArrayList<>();
            for(QueryTerm q:query){
                terms.add(q.getTerm());
            }
            retrievedList = getDocInfoEssentialForTerms(terms);
            System.out.println(retrievedList.size());
            for (List<DocInfoEssential> ls : retrievedList) {
                for (DocInfoEssential obj : ls) {
                    returnList.add(new Pair<>(obj, 1.0));
                }
            }
        }
        else {
            List<List<DocInfoEssential>> retrievedList;
            List<String> terms=new ArrayList<>();
            for(QueryTerm q:query){
                terms.add(q.getTerm());
            }
            retrievedList = getDocInfoEssentialForTerms(terms);
            System.out.println(retrievedList.size());
            for (List<DocInfoEssential> ls : retrievedList) {
                for (DocInfoEssential obj : ls) {
                    returnList.add(new Pair<>(obj.getId(), 1.0));
                }
            }
        }
        List<Pair<Object, Double>> finalreturnList=new LinkedList<>();
        for(int i=0;i<topk;i++){
            finalreturnList.add(returnList.get(i));
        }
        return finalreturnList;

    }

}
