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

//import com.sun.imageio.plugins.common.SingleTileRenderedImage;
import gr.csd.uoc.hy463.themis.indexer.Indexer;
import gr.csd.uoc.hy463.themis.indexer.model.DocInfoEssential;
import gr.csd.uoc.hy463.themis.retrieval.QueryTerm;
import gr.csd.uoc.hy463.themis.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static gr.csd.uoc.hy463.themis.indexer.Indexer.*;

/**
 * Implementation of the OkapiBM25 retrieval model
 *
 * @author Panagiotis Papadakos <papadako at ics.forth.gr>
 */
class Infos{
    List<String> docTerms=new ArrayList<>();
    double score=0;
    double norm=0;
}
class smallData{
    String docid;
    double weight;
}
public class VSM extends ARetrievalModel {

    String path;
    public VSM(Indexer index) throws IOException {
        super(index);
        index.init();
        index.load();
        path=index.__INDEX_PATH__;
    }

    @Override
    public  List<Pair<Object, Double>> getRankedResults(List<QueryTerm> query, RESULT_TYPE type) throws IOException {
        List<Pair<Object, Double>> returnList=new LinkedList<>();
        double N=0;
        Scanner myReader = new Scanner(new File(path+"infos.idx"));
        while (myReader.hasNextLine()) {
            String[] data = myReader.nextLine().split("=");
            if(data[0].equals("totalNumofArticles")){
                N=Double.parseDouble(data[1]);
            }
        }
//        System.out.println("N="+N);
        myReader.close();
//        System.out.println("N++ "+N);
        HashMap<String,Double> queryWeights=new HashMap<>();
        List<List<DocInfoEssential>> retrievedList;
        List<String> terms=new ArrayList<>();
        //upologizei to kanoniko weight

        //------calc the weight in query----------
        for(QueryTerm q:query){
            double tf=q.getWeight();

            if(__VOCABULARY__.containsKey(q.getTerm())) {

                int df = __VOCABULARY__.get(q.getTerm()).getL();

                double idf = Math.log(N / df) / Math.log(2);

                double weight = tf * idf;

                q.setWeight(weight);
                terms.add(q.getTerm());
                queryWeights.put(q.getTerm(), q.getWeight());
            }
        }
        //-----edn of calc of weight----------


        HashMap<String,HashMap<String,Double>> termWeightsMap=new HashMap<>();  //gia kathe le3i krataw mia domh pou
                                                                                // mas leei gia kathe keimeno poio einai
                                                                                //to varos ths


//      tha krathsoume se mia domh olous tous pointers gia ta docids
//      kanontas (pointer div BufferSize) tha kserw se poio block toy
//      DocumentFile einai apothhkeumenh h eggrafh gia auto to docid
//      sto opoio antistoixei o pointer

//        for(String str:terms){
//            int df=__VOCABULARY__.get(str).getL();
//            long pointer=__VOCABULARY__.get(str).getR();
//            __POSTINGS__.seek(pointer);
//            double idf=Math.log(N/df)/Math.log(2);
//            byte[] bytes=new byte[df*56];
//            __POSTINGS__.read(bytes);
//        }


        List<Long> listofpointer=new LinkedList<>();


        for(String str:terms){
            int df=__VOCABULARY__.get(str).getL();
            long pointer=__VOCABULARY__.get(str).getR();
            __POSTINGS__.seek(pointer);
            double idf=Math.log(N/df)/Math.log(2);
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
                String docid=new String(id);
                double tf= ByteBuffer.wrap(_tf_).getDouble();
                long doc_pointer=ByteBuffer.wrap(_pointer_).getLong();
                listofpointer.add(doc_pointer);
                double weight=tf*idf;
                if(termWeightsMap.containsKey(str)){
                    HashMap<String,Double> map=termWeightsMap.get(str);
                    map.put(docid,weight);
                    termWeightsMap.put(str,map);
                }else{
                    HashMap<String,Double> map=new HashMap<>();
                    map.put(docid,weight);
                    termWeightsMap.put(str,map);
                }

            }
        }

//        se auto to map tha apothhkeuetai o arithmous toy block sto opoio
//        tha vrethei o pointer wste na to anasuroume oloklhro
//        me to arrayist loipon tha kseroume oti travontas ayto to block
//        tha mporesoume na anasuroyme plhroforiea gia ta docids twn opoiwn oi
//        pointers vriskontai mesa se ayth. Auto tha ginei kanontas x=(pointer mod bufferSize)
//        kai kanontas skip x bytes apo thn arxh tou
//
//        TreeMap<Integer,ArrayList<Long>> map_of_blocks=new TreeMap<>() ;
//        for(long p:listofpointer){
//            int numofblock=(int) p/_MAX_NUMBER_OF_BYTES_PER_BLOCK_;
//            if(map_of_blocks.containsKey(numofblock)){
//                ArrayList<Long> list=map_of_blocks.get(numofblock);
//                list.add(p);
//                map_of_blocks.put(numofblock,list);
//            }
//            else{
//                ArrayList<Long> list=new ArrayList<>();
//                list.add(p);
//                map_of_blocks.put(numofblock,list);
//            }
//        }







        double queryNorm=0;
        for(QueryTerm q:query){
//            System.out.println("------"+q.getWeight());
            queryNorm=queryNorm+Math.pow(q.getWeight(),2);
//            System.out.println("tha mpei edw? "+q.getWeight()+" , "+q.getTerm());
        }

        queryNorm=Math.sqrt(queryNorm);
//        System.out.println("query Norm= "+queryNorm);
        retrievedList = getDocInfoEssentialForTerms(terms);

        /*
        * se auto to shmieo exoyme gia kathe oro se poia docs vrethke
        * gia kathe ena apo ayta ta doc exoyme mesw to DocInfoEssential
        * -offset dld ton pointer
        * -ena map apo to prop pou exei to length,to pagerank(0.0) kai th norma tou doc
        * -to id tou doc
        * auta mporoume na taparoume apo to obj
        *
        * Twra tha antistrepsoume auth th plhroforia kai tha ftiaxoyme mia domi me objects me pedia
        * -ena keimeno
        * -ti lista apo tis lekseis toy query poy periexontai se auto to keimeno
        * -to score toy keimenou poy tha upologistei vash toy cosSim
        * */
        HashMap<String,Infos> infosMap=new HashMap<>(); //docid,infos ----koita panw
        for (int i=0;i<retrievedList.size();i++) {
            for (DocInfoEssential obj : retrievedList.get(i)) {
                if(!infosMap.containsKey(obj.getId())) {
                    Infos infos = new Infos();
                    infos.docTerms.add(terms.get(i));//oi list of terms kai h retrivedList einai 1-1
                    infos.score = 0.0;
                    infos.norm= (double) obj.getProperty(DocInfoEssential.PROPERTY.WEIGHT);
//                    System.out.println("infos.norm (1)"+ infos.norm);
                    infosMap.put(obj.getId(), infos);
                }
                else{
                    Infos infos=infosMap.get(obj.getId());
                    infos.docTerms.add(terms.get(i));
                    //infos.norm= (double) obj.getProperty(DocInfoEssential.PROPERTY.WEIGHT);
//                    System.out.println("infos.norm (2)"+ infos.norm);
                    infosMap.put(obj.getId(), infos);
                }

            }
        }
        /*
        * twra eimaste etoimoi na upologisoume to score tou kathe docID
        * */
        for(String docID:infosMap.keySet()){
            Infos infos=infosMap.get(docID);
            double numerator=0;
            for(String term:infos.docTerms){
                numerator+=( (termWeightsMap.get(term).get(docID)) * queryWeights.get(term) );
//                if(term.equals("lydia")){
//                    System.out.println("1. "+numerator);
//                    System.out.println("1a."+termWeightsMap.get(term).get(docID));
//                    System.out.println("1b."+queryWeights.get(term));
//                }

            }
//            System.out.println("numerator "+numerator);
//            System.out.println("infos.norm "+infos.norm);
//            System.out.println("queryNorm "+queryNorm);
//            System.out.println("infos.norm*qyeryNorm "+infos.norm*queryNorm);
            double score=numerator/(infos.norm*queryNorm);
            returnList.add(new Pair<>(docID,score));

        }

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
        int N=0;
        Scanner myReader = new Scanner(new File(path+"infos.idx"));
        while (myReader.hasNextLine()) {
            String[] data = myReader.nextLine().split("=");
            if(data[0].equals("totalNumofArticles")){
                N=Integer.parseInt(data[1]);
            }
        }
        myReader.close();
//        System.out.println("N++ "+N);
        HashMap<String,Double> queryWeights=new HashMap<>();
        List<List<DocInfoEssential>> retrievedList;
        List<String> terms=new ArrayList<>();
        //upologizei to kanoniko weight
        for(QueryTerm q:query){
            double tf=q.getWeight();
//            System.out.println("->"+q.getTerm());
//            System.out.println("size->"+__VOCABULARY__.size());
            if(__VOCABULARY__.containsKey(q.getTerm())){
//                System.out.println("mplamplampla");
            }
            int df=__VOCABULARY__.get(q.getTerm()).getL();
//            System.out.println("df++ "+df);
            double idf= Math.log(N/df)/ Math.log(2);
//            System.out.println("idf++ "+idf);
            double weight=tf*idf;
//            System.out.println("weight++ "+weight);
            q.setWeight(weight);
            terms.add(q.getTerm());
            queryWeights.put(q.getTerm(),q.getWeight());
        }

        HashMap<String,HashMap<String,Double>> termWeightsMap=new HashMap<>();

        for(String str:terms){
            int df=__VOCABULARY__.get(str).getL();
            long pointer=__VOCABULARY__.get(str).getR();
            __POSTINGS__.seek(pointer);
            double idf=Math.log(N/df)/Math.log(2);
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
                String docid=new String(id);
                double tf= ByteBuffer.wrap(_tf_).getDouble();
                //long pointer=ByteBuffer.wrap(_pointer_).getLong();
                double weight=tf*idf;
                if(termWeightsMap.containsKey(str)){
                    HashMap<String,Double> map=termWeightsMap.get(str);
                    map.put(docid,weight);
                    termWeightsMap.put(str,map);
                }else{
                    HashMap<String,Double> map=new HashMap<>();
                    map.put(docid,weight);
                    termWeightsMap.put(str,map);
                }

            }
        }


        double queryNorm=0;
        for(QueryTerm q:query){
//            System.out.println("------"+q.getWeight());
            queryNorm=queryNorm+Math.pow(q.getWeight(),2);
        }

        queryNorm=Math.sqrt(queryNorm);
//        System.out.println("query Norm= "+queryNorm);
        retrievedList = getDocInfoEssentialForTerms(terms);

        /*
         * se auto to shmieo exoyme gia kathe oro se poia docs vrethke
         * gia kathe ena apo ayta ta doc exoyme mesw to DocInfoEssential
         * -offset dld ton pointer
         * -ena map apo to prop pou exei to length,to pagerank(0.0) kai th norma tou doc
         * -to id tou doc
         * auta mporoume na taparoume apo to obj
         *
         * Twra tha antistrepsoume auth th plhroforia kai tha ftiaxoyme mia ArrayList me objects me pedia
         * -ena keimeno
         * -ti lista apo tis lekseis toy query poy periexontai se auto to keimeno
         * -to score toy keimenou poy tha upologistei vash toy cosSim
         * */
        HashMap<String,Infos> infosMap=new HashMap<>();
        for (int i=0;i<retrievedList.size();i++) {
            for (DocInfoEssential obj : retrievedList.get(i)) {
                if(!infosMap.containsKey(obj.getId())) {
                    Infos infos = new Infos();
                    infos.docTerms.add(terms.get(i));//oi list of terms kai h retrivedList einai 1-1
                    infos.score = 0.0;
                    infos.norm= (double) obj.getProperty(DocInfoEssential.PROPERTY.WEIGHT);
//                    System.out.println("infos.norm (1)"+ infos.norm);
                    infosMap.put(obj.getId(), infos);
                }
                else{
                    Infos infos=infosMap.get(obj.getId());
                    infos.docTerms.add(terms.get(i));
                    infos.norm= (double) obj.getProperty(DocInfoEssential.PROPERTY.WEIGHT);
//                    System.out.println("infos.norm (2)"+ infos.norm);
                    infosMap.put(obj.getId(), infos);
                }

            }
        }
        /*
         * twra eimaste etoimoi na upologisoume to score tou kathe docID
         * */
        for(String docID:infosMap.keySet()){
            Infos infos=infosMap.get(docID);
            double numerator=0;
            for(String term:infos.docTerms){
                numerator+=( (termWeightsMap.get(term).get(docID)) * queryWeights.get(term) );
//                if(term.equals("lydia")){
//                    System.out.println("1. "+numerator);
//                    System.out.println("1a."+termWeightsMap.get(term).get(docID));
//                    System.out.println("1b."+queryWeights.get(term));
//                }

            }
//            System.out.println("numerator "+numerator);
//            System.out.println("infos.norm "+infos.norm);
//            System.out.println("queryNorm "+queryNorm);
//            System.out.println("infos.norm*qyeryNorm "+infos.norm*queryNorm);

            double score=numerator/(infos.norm*queryNorm);
//            System.out.println("score "+score);
            returnList.add(new Pair<>(docID,score));

        }

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
