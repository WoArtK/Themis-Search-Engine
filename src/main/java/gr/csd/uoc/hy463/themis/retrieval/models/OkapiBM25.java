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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static gr.csd.uoc.hy463.themis.indexer.Indexer.*;

/**
 * Implementation of the OkapiBM25 retrieval model
 *
 * @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */


class dataforOkabi{
    String term;
    double tf;
}



public class OkapiBM25 extends ARetrievalModel {
    String path;
    public OkapiBM25(Indexer index) throws IOException {
        super(index);
        index.init();
        index.load();
        path=index.__INDEX_PATH__;


    }

    @Override
    public  List<Pair<Object, Double>> getRankedResults(List<QueryTerm> query, RESULT_TYPE type) throws IOException {
        List<Pair<Object, Double>> returnList=new LinkedList<>();
        double k=2.0;
        double b=0.75;
        double averageLength=0;
        double N=0;
        Scanner myReader = new Scanner(new File(path+"infos.idx"));
        while (myReader.hasNextLine()) {
            String[] data = myReader.nextLine().split("=");
            if(data[0].equals("averageLength")){
                averageLength=Double.parseDouble(data[1]);
            }
            else if(data[0].equals("totalNumofArticles")){
                N=Double.parseDouble(data[1]);
            }
            else {}
        }
        myReader.close();
        /*
        * edw tha ftia3oume mia domi hashmap<docID,Arralist<{term,tf}>>
        * etsi diasxizontas auto to hashmap tha exoume gia kathe keimeno
        * to tf tou kathe term toy query sth sullogh(zeiteitai gia to okapibm25)
        *
        * epishs xreiazomaste to df tou kathe term. se auto exoume access me o(1)
        * apo to __VOCABULARY__
        *
        * */
        List<String> querytermsforIndexer=new ArrayList<>();
        HashMap<String,Double> idfHashMap=new HashMap<>();
        HashMap<String, ArrayList<dataforOkabi>> termofDoc=new HashMap<>();
        for(QueryTerm queryTerm:query){
            String term=queryTerm.getTerm();
            querytermsforIndexer.add(term);
            if(__VOCABULARY__.containsKey(term)){
                int df=__VOCABULARY__.get(term).getL();
                long pointerInPosting=__VOCABULARY__.get(term).getR();
                __POSTINGS__.seek(pointerInPosting);
                //---------Calculate IDF------------------
                double numerator=N-df+0.5;
                double denominator=df+0.5;
                double idf=Math.log(numerator/denominator)/Math.log(2);
                idfHashMap.put(term,idf);
                //--------end of calculation-------------
                byte[] bytes=new byte[df*56];
                __POSTINGS__.read(bytes);
                int offset=0;
                for(int i=0;i<df;i++){
                    //String[] postLine=__POSTINGS__.readUTF().split("\t");
                    byte[] id=Arrays.copyOfRange(bytes,offset,offset+40);
                    offset+=40;
                    byte[] _tf_=Arrays.copyOfRange(bytes,offset,offset+8);
                    offset+=8;
                    byte[] _pointer_=Arrays.copyOfRange(bytes,offset,offset+8);
                    offset+=8;
                    String docID=new String(id);
                    double _tf= ByteBuffer.wrap(_tf_).getDouble();
                    long pointer=ByteBuffer.wrap(_pointer_).getLong();

                    if(termofDoc.containsKey(docID)){
                        ArrayList<dataforOkabi> tmp=termofDoc.get(docID);
                        dataforOkabi toinsert = new dataforOkabi();
                        toinsert.term=term;
                        toinsert.tf=_tf;
                        tmp.add(toinsert);
                        termofDoc.put(docID,tmp);
                    }else{
                        ArrayList<dataforOkabi> tmp=new ArrayList<>();
                        dataforOkabi toinsert=new dataforOkabi();
                        toinsert.term=term;
                        toinsert.tf=_tf;
                        tmp.add(toinsert);
                        termofDoc.put(docID,tmp);
                    }
                }
            }
        }

        //---------------get length of documents--------------
        HashMap<String,Integer> lengthofDoc=new HashMap<>();
        List<List<DocInfoEssential>> retrievedList=getDocInfoEssentialForTerms(querytermsforIndexer);
        for(List<DocInfoEssential> firstList:retrievedList){
            for(DocInfoEssential ob:firstList){
                if(!lengthofDoc.containsKey(ob.getId())){
                    lengthofDoc.put(ob.getId(),(int) ob.getProperty(DocInfoEssential.PROPERTY.LENGTH));
                }
            }
        }

        //-----------------------------------------------------

        //--------okapiBM25 docID score calculation-----------------
        for(String docID:termofDoc.keySet()){
            double score=0;
            ArrayList<dataforOkabi> termsListindoc=termofDoc.get(docID);
            for(dataforOkabi ob:termsListindoc){
                double numerator=ob.tf*(k+1);
                double denominator=ob.tf+k*(1-b+(b*(lengthofDoc.get(docID)/averageLength)));
                double secondpart=numerator/denominator;
                double partscore=idfHashMap.get(ob.term)*secondpart;
                score+=partscore;
            }
            returnList.add(new Pair<>(docID,Math.abs(score)));
        }
        //----------------------------------------------------------
        Collections.sort(returnList,
                new Comparator<Pair<Object, Double>>() {
                    @Override
                    public int compare(final Pair<Object, Double> o1, final Pair<Object, Double> o2) {
                        if(o1.getR() > o2.getR()){
                            return -1;
                        }
                        else if(o1.getR() < o2.getR()){
                            return 1;
                        }
                        return 0;
                    }
                });


        return returnList;
    }

    @Override
    public List<Pair<Object, Double>> getRankedResults(List<QueryTerm> query, RESULT_TYPE type, int topk) throws IOException {
        List<Pair<Object, Double>> returnList=new LinkedList<>();
        double k=2.0;
        double b=0.75;
        double averageLength=0;
        int N=0;
        Scanner myReader = new Scanner(new File(path+"infos.idx"));
        while (myReader.hasNextLine()) {
            String[] data = myReader.nextLine().split("=");
            if(data[0].equals("averageLength")){
                averageLength=Double.parseDouble(data[1]);
            }
            else if(data[0].equals("totalNumofArticles")){
                N=Integer.parseInt(data[1]);
            }
            else {}
        }
        myReader.close();
        /*
         * edw tha ftia3oume mia domi hashmap<docID,Arralist<{term,tf}>>
         * etsi diasxizontas auto to hashmap tha exoume gia kathe keimeno
         * to tf tou kathe term toy query sth sullogh(zeiteitai gia to okapibm25)
         *
         * epishs xreiazomaste to df tou kathe term. se auto exoume access me o(1)
         * apo to __VOCABULARY__
         *
         * */
        List<String> querytermsforIndexer=new ArrayList<>();
        HashMap<String,Double> idfHashMap=new HashMap<>();
        HashMap<String, ArrayList<dataforOkabi>> termofDoc=new HashMap<>();
        for(QueryTerm queryTerm:query){
            String term=queryTerm.getTerm();
            querytermsforIndexer.add(term);
            if(__VOCABULARY__.containsKey(term)){
                int df=__VOCABULARY__.get(term).getL();
                long pointerInPosting=__VOCABULARY__.get(term).getR();
                __POSTINGS__.seek(pointerInPosting);
                //---------Calculate IDF------------------
                double numerator=N-df+0.5;
                double denominator=df+0.5;
                double idf=Math.log(numerator/denominator)/Math.log(2);
                idfHashMap.put(term,idf);
                //--------end of calculation-------------
                byte[] bytes=new byte[df*56];
                __POSTINGS__.read(bytes);
                int offset=0;
                for(int i=0;i<df;i++){
                    byte[] id=Arrays.copyOfRange(bytes,offset,offset+40);
                    offset+=40;
                    byte[] _tf_=Arrays.copyOfRange(bytes,offset,offset+8);
                    offset+=8;
                    byte[] _pointer_=Arrays.copyOfRange(bytes,offset,offset+8);
                    offset+=8;
                    String docID=new String(id);
                    double _tf= ByteBuffer.wrap(_tf_).getDouble();
                    long pointer=ByteBuffer.wrap(_pointer_).getLong();
                    if(termofDoc.containsKey(docID)){
                        ArrayList<dataforOkabi> tmp=termofDoc.get(docID);
                        dataforOkabi toinsert = new dataforOkabi();
                        toinsert.term=term;
                        toinsert.tf=_tf;
                        tmp.add(toinsert);
                        termofDoc.put(docID,tmp);
                    }else{
                        ArrayList<dataforOkabi> tmp=new ArrayList<>();
                        dataforOkabi toinsert=new dataforOkabi();
                        toinsert.term=term;
                        toinsert.tf=_tf;
                        tmp.add(toinsert);
                        termofDoc.put(docID,tmp);
                    }
                }
            }
        }

        //---------------get length of documents--------------
        HashMap<String,Integer> lengthofDoc=new HashMap<>();
        List<List<DocInfoEssential>> retrievedList=getDocInfoEssentialForTerms(querytermsforIndexer);
        for(List<DocInfoEssential> firstList:retrievedList){
            for(DocInfoEssential ob:firstList){
                if(!lengthofDoc.containsKey(ob.getId())){
                    lengthofDoc.put(ob.getId(),(int) ob.getProperty(DocInfoEssential.PROPERTY.LENGTH));
                }
            }
        }

        //-----------------------------------------------------

        //--------okapiBM25 docID score calculation-----------------
        for(String docID:termofDoc.keySet()){
            double score=0;
            ArrayList<dataforOkabi> termsListindoc=termofDoc.get(docID);
            for(dataforOkabi ob:termsListindoc){
                double numerator=ob.tf*(k+1);
                double denominator=ob.tf+k*(1-b+(b*(lengthofDoc.get(docID)/averageLength)));
                double secondpart=numerator/denominator;
                double partscore=idfHashMap.get(ob.term)*secondpart;
                score+=partscore;
            }
            returnList.add(new Pair<>(docID,score));
        }
        //----------------------------------------------------------
        Collections.sort(returnList,
                new Comparator<Pair<Object, Double>>() {
                    @Override
                    public int compare(final Pair<Object, Double> o1, final Pair<Object, Double> o2) {
                        if(o1.getR() > o2.getR()){
                            return -1;
                        }
                        else if(o1.getR() < o2.getR()){
                            return 1;
                        }
                        return 0;
                    }
                });

        List<Pair<Object, Double>> finalreturnList=new LinkedList<>();
        for(int i=0;i<topk;i++){
            finalreturnList.add(returnList.get(i));
        }
        return finalreturnList;
    }
}
