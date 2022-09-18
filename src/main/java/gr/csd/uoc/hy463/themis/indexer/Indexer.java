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
package gr.csd.uoc.hy463.themis.indexer;

import gr.csd.uoc.hy463.themis.config.Config;
import gr.csd.uoc.hy463.themis.indexer.indexes.BigData;
import gr.csd.uoc.hy463.themis.indexer.indexes.Index;
import gr.csd.uoc.hy463.themis.indexer.model.DocInfoEssential;
import gr.csd.uoc.hy463.themis.indexer.model.DocInfoFull;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.collections.SemanticScholar.S2JsonEntryReader;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.collections.SemanticScholar.S2TextualEntry;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.stemmer.Stemmer;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.stemmer.StopWords;
import gr.csd.uoc.hy463.themis.utils.Pair;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLOutput;
import java.util.*;
import java.util.concurrent.TimeUnit;

import it.unimi.dsi.fastutil.Hash;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.spans.SpanWeight;
import org.bytedeco.opencv.presets.opencv_core;
import org.nd4j.linalg.exception.ND4UnresolvedOutputVariables;
import scala.Int;
import scala.concurrent.duration.Duration;
import scala.math.Integral;

import javax.print.Doc;

import static gr.csd.uoc.hy463.themis.lexicalAnalysis.stemmer.StopWords.__WORDS__;
import static java.nio.ByteBuffer.*;

/**
 * Our basic indexer class. This class is responsible for two tasks:
 * <p>
 * a) Create the appropriate indexes given a specific directory with files (in
 * our case the Semantic Scholar collection)
 * <p>
 * b) Given a path load the indexes (if they exist) and provide information
 * about the indexed data, that can be used for implementing any kind of
 * retrieval models
 * <p>
 * When the indexes have been created we should have three files, as documented
 * in Index.java
 *
 * @author Panagiotis Papadakos (papadako@ics.forth.gr)
 */


public class Indexer {

    private static final Logger __LOGGER__ = LogManager.getLogger(Indexer.class);
    private Config __CONFIG__;  // configuration options
    // The file path of indexes
    public String __INDEX_PATH__ = null;
    // Filenames of indexes
    private String __VOCABULARY_FILENAME__ = null;
    private String __POSTINGS_FILENAME__ = null;
    private String __DOCUMENTS_FILENAME__ = null;
    private String __META_FILENAME__ = null;
    //contains all the absolute paths of files in a folder and its subfolders
    public static ArrayList<String> absolutePaths = new ArrayList<>();
    public static int Merged_Voc_File_Counter = 0;
    public static int Merged_Post_File_Counter = 0;
    public static int totalNumofArticles = 0;

    public static HashMap<String, Long> indexing_PointersToDocFile = new HashMap<>();
    public static HashMap<String,Integer> mymap=new HashMap<>();
    // Vocabulary should be stored in memory for querying! This is crucial
    // since we want to keep things fast! This is done through load().
    // For this project use a HashMap instead of a trie
    public static HashMap<String, Pair<Integer, Long>> __VOCABULARY__ = null;
    public static RandomAccessFile __POSTINGS__ = null;
    public static RandomAccessFile __DOCUMENTS__ = null;
    public static long _MAX_NUMBER_OF_BYTES_PER_BLOCK_=68*4096;

    // This map holds any information related with the indexed collection
    // and should be serialized when the index process has finished. Such
    // information could be the avgDL for the Okapi-BM25 implementation,
    // a timestamp of when the indexing process finished, the path of the indexed
    // collection, the options for stemming and stop-words used in the indexing process,
    // and whatever else you might want. But make sure that before querying
    // the serialized file is loaded
    private Map<String, String> __META_INDEX_INFO__ = null;

    /**
     * Default constructor. Creates also a config instance
     *
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Indexer() throws IOException, ClassNotFoundException {
        __CONFIG__ = new Config();  // reads info from themis.config file
        init();
    }

    /**
     * Constructor that gets a current Config instance
     *
     * @param config
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public Indexer(Config config) throws IOException, ClassNotFoundException {
        this.__CONFIG__ = config;  // reads info from themis.config file
        init();
    }

    /**
     * Initialize things
     */
    public void init() {
        __VOCABULARY_FILENAME__ = __CONFIG__.getVocabularyFileName();
        __POSTINGS_FILENAME__ = __CONFIG__.getPostingsFileName();
        __DOCUMENTS_FILENAME__ = __CONFIG__.getDocumentsFileName();
        __INDEX_PATH__ = __CONFIG__.getIndexPath();
    }

    /**
     * Checks that the index path + all *.idx files exist
     * <p>
     * Method that checks if we have all appropriate files
     *
     * @return
     */
    public boolean hasIndex() {
        // Check if path exists
        File file = new File(__INDEX_PATH__);
        if (!file.exists() || !file.isDirectory()) {
            __LOGGER__.error(__INDEX_PATH__ + "directory does not exist!");
            return false;
        }
        // Check if index files exist
        file = new File(__INDEX_PATH__ + __VOCABULARY_FILENAME__);
        if (!file.exists() || file.isDirectory()) {
            __LOGGER__.error(__VOCABULARY_FILENAME__ + "vocabulary file does not exist in " + __INDEX_PATH__);
            return false;
        }
        file = new File(__INDEX_PATH__ + __POSTINGS_FILENAME__);
        if (!file.exists() || file.isDirectory()) {
            __LOGGER__.error(__POSTINGS_FILENAME__ + " posting binary file does not exist in " + __INDEX_PATH__);
            return false;
        }
        file = new File(__INDEX_PATH__ + __DOCUMENTS_FILENAME__);
        if (!file.exists() || file.isDirectory()) {
            __LOGGER__.error(__DOCUMENTS_FILENAME__ + "documents binary file does not exist in " + __INDEX_PATH__);
            return false;
        }
        return true;
    }


    public static void listFilesForFolder(final File folder) {
        System.out.println("---------start of listing paths---------");
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                System.out.println("is a Direcotry: " + fileEntry.getName());
                //listFilesForFolder(fileEntry);
            } else {
                //System.out.println(fileEntry.getName());
//                String[] str=fileEntry.getName().split("-");
//                if(str[0].equals("s2") && str[1].equals("corpus")){
                absolutePaths.add(fileEntry.getAbsolutePath());
                System.out.println(fileEntry.getName());
                //}

            }
        }
        System.out.println("---------end of listing paths---------");

    }



    public void Return_Words_Map(StringTokenizer line){
        while(line.hasMoreTokens()){
            String[] terms = line.nextToken().split("\\s+");
            for (String term : terms) {
                if (mymap.containsKey(term)) {
                    int counter=mymap.get(term);
                    counter++;
                    mymap.put(term,counter);

                } else {
                    if(!__WORDS__.contains(term)) {
                        mymap.put(term, 1);
                    }
                }

            }
        }
    }


    /**
     * Method responsible for indexing a directory of files
     * <p>
     * If the number of files is larger than the PARTIAL_INDEX_MAX_DOCS_SIZE set
     * to the themis.config file then we have to dump all data read up to now to
     * a partial index and continue with a new index. After creating all partial
     * indexes then we have to merge them to create the final index that will be
     * stored in the file path.
     * <p>
     * Can also be modified to use the MAX_MEMORY usage parameter given in
     * themis.conf for brave hearts!
     *
     * @param folderPath
     * @return
     * @throws IOException
     */
    public boolean index(String folderPath) throws IOException {
//        String[] oldfilesPath=new String[4];
//        oldfilesPath[0]=__INDEX_PATH__+__POSTINGS_FILENAME__;
//        oldfilesPath[1]=__INDEX_PATH__+__VOCABULARY_FILENAME__;
//        oldfilesPath[2]=__INDEX_PATH__+__DOCUMENTS_FILENAME__;
//        oldfilesPath[3]=__INDEX_PATH__+"infos.idx";
//        for(int i=0;i<4;i++){
//            File myObj=new File(oldfilesPath[i]);
//            if (myObj.delete()) {
//                System.out.println("Deleted the file: " + myObj.getName());
//            } else {
//                System.out.println("Failed to delete the file.");
//            }
//        }


        System.out.println("index Start");
        Index index = new Index(__CONFIG__);
        String[] arg=new String[1];
        StopWords.main(arg);
        int partialIndexes = 0;
        String docPath = __INDEX_PATH__ + __DOCUMENTS_FILENAME__;
        RandomAccessFile DocFile = new RandomAccessFile(docPath, "rw");
        long docFile_seek = 0;

        Stemmer.Initialize();
        StopWords.Initialize();
        double sumForAverageLength = 0;
        listFilesForFolder(new File(folderPath));
        /*
         * N is the total number of docs. The calculating cost is low and we will use it to calculate the
         * weight terms for norm on the fly
         * */
        int N = 0;
        int articles = 0;
        // for each file in path
        long startTime = System.nanoTime();
        int cur_Path_counter = 0;
        long buffer = 0;
        System.out.println("__CONFIG__.getPartialIndexSize() : "+__CONFIG__.getPartialIndexSize() );
        ByteBuffer bb= ByteBuffer.allocate(__CONFIG__.getPartialIndexSize() * 68);
        long docFile_pointer=0;
        for (String path : absolutePaths) {
            cur_Path_counter++;
            System.out.println("indexing file= " + path);
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();

            while (line != null) {
                double max_fieldfreq = 1;
                N++;
                //System.out.println(N);
                totalNumofArticles++;
                //-----auta tha ginon mpoun sto DocFile--------
                String str_docID = null;
                String str_title = null;
                ArrayList<String> auhtors = new ArrayList<>();
                ArrayList<String> authorsIDs = new ArrayList<>();
                String str_year = null;
                String str_journal = null;
                //--------------------------------------------

                int str_length = 0;

                articles++;

                S2TextualEntry textualEntry;
                textualEntry = S2JsonEntryReader.readTextualEntry(line);

                str_title = textualEntry.getTitle().replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();
                Return_Words_Map( new StringTokenizer(str_title));

                StringTokenizer abstract__st;
                abstract__st = new StringTokenizer(textualEntry.getPaperAbstract().replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(abstract__st);



                str_journal = textualEntry.getJournalName().replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();

                Return_Words_Map(new StringTokenizer(str_journal));

                StringTokenizer year__st;
                int year = textualEntry.getYear();
                str_year = Integer.toString(textualEntry.getYear());
                year__st = new StringTokenizer(str_year.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(year__st);

                StringTokenizer venue__st;
                venue__st = new StringTokenizer(textualEntry.getVenue().replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(venue__st);

                for (String str : textualEntry.getEntities()) {
                    StringTokenizer entities__st;
                    entities__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(entities__st);
                }

//                for(String str:textualEntry.getCitations()){
//                    String citation_title=getCitation_title(str);
//                    StringTokenizer citations__st;
//                    citations__st = new StringTokenizer(citation_title.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
//                    Return_Words_Map(citations__st);
//                }


                for (String str : textualEntry.getFieldsOfStudy()) {
                    StringTokenizer fieldsOfStudy__st;
                    fieldsOfStudy__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(fieldsOfStudy__st);
                }

                for (String str : textualEntry.getSources()) {
                    StringTokenizer sources__st;
                    sources__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(sources__st);
                }

                List<Pair<String, List<String>>> authors = textualEntry.getAuthors();
                for (int i = 0; i < authors.size(); i++) {
                    String author = authors.get(i).getL();
                    StringTokenizer author__st;
                    String a = author.replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();
                    auhtors.add(a);
                    for (int j = 0; j < authors.get(i).getR().size(); j++) {
                        String str = authors.get(i).getR().get(j).replaceAll("[^a-zA-Z0-9]", " ")
                                .replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();

                        authorsIDs.add(str);
                    }
                    author__st = new StringTokenizer(a);
                    Return_Words_Map(author__st);
                }

                str_docID = textualEntry.getId();

                for(String term:mymap.keySet()){
                    String stemmed_term = Stemmer.Stem(term);

                    if (index.BigWordsMap.containsKey(stemmed_term)) { // den uparxei sto map
                        BigData bd = index.BigWordsMap.get(stemmed_term);
                        if (!bd.mymap.containsKey(str_docID)) {
                            bd.df++;
                            str_length += mymap.get(term);

                            bd.mymap.put(str_docID, (double)mymap.get(term));
                            index.BigWordsMap.put(stemmed_term, bd);
                        } else {
                            str_length += mymap.get(term);

                            double tf = bd.mymap.get(str_docID);
                            tf += (double) mymap.get(term);
                            bd.mymap.put(str_docID, tf);
                            index.BigWordsMap.put(stemmed_term, bd);
                        }
                    }
                    else{
                        BigData bd=new BigData();
                        bd.df = 1;
                        str_length += mymap.get(term);

                        bd.mymap.put(str_docID,(double)mymap.get(term));
                        index.BigWordsMap.put(stemmed_term,bd);
                    }
                    double cur_freq=index.BigWordsMap.get(stemmed_term).mymap.get(str_docID);
                    if (cur_freq > max_fieldfreq) {
                        max_fieldfreq = cur_freq;
                    }
                }


                //gia ka8e le3i, an sto pedio mymap.contains( Doc me id = textualEntry.getId() ) prepei na pao kai na enimeroso to TF
                for (String s : mymap.keySet()) {
                    s = Stemmer.Stem(s);
                    if (index.BigWordsMap.get(s).mymap.containsKey(str_docID)) {
                        if(max_fieldfreq==0) System.out.println("max field freq is 0");
                        double tf = index.BigWordsMap.get(s).mymap.get(str_docID) / max_fieldfreq;
                        if(tf== Double.POSITIVE_INFINITY);
                        index.BigWordsMap.get(s).mymap.put(str_docID, tf);
                    }
                }




                index.PointersToDocFile.put(str_docID, docFile_pointer);
                docFile_pointer += 68;
//                DocFile.writeUTF(str_docID);
//                //DocFile.writeUTF(str_title);
//                //DocFile.writeUTF(authorstmp);
//                //DocFile.writeUTF(idtmp);
//                //DocFile.writeUTF(str_year);
//                //DocFile.writeUTF(str_journal);
//                DocFile.writeUTF("1000000000");
//                DocFile.writeUTF(Integer.toString(str_length));
//                DocFile.writeUTF("0000000000");
//                DocFile.writeUTF("2000000000");
//                //DocFile.writeUTF("\n");

                bb.put(str_docID.getBytes()); //40 Bytes
                bb.putDouble(0.0);          //8 Bytes
                bb.putInt(str_length);      //4 Bytes
                bb.putDouble(0.0);          //8 Bytes
                bb.putDouble(0.0);          //8 Bytes

                sumForAverageLength += str_length;

                mymap.clear();
                line = reader.readLine();
                if (articles == __CONFIG__.getPartialIndexSize() || (cur_Path_counter == absolutePaths.size() && line == null)) {
                    DocFile.write(bb.array());
                    bb.clear();
                    System.out.println("partialNumber=" + partialIndexes);
                    // Increase partial indexes and dump files to appropriate directory
                    System.out.println("articles=" + articles);
                    articles = 0;
                    //System.out.println(partialIndexes+", "+articles);
                    partialIndexes++;
                    index.setID(partialIndexes);
                    index.dump();   // dump partial index to appropriate subdirectory(
                    // tha paei na grapsei ta antistoixa vod kai post files)

                }
//                docFile_pointer += 68;

            }
            reader.close();
        }

//        index.setID(0);
//        index.dump();
        DocFile.close();


        double averageLength = sumForAverageLength / totalNumofArticles;

        FileWriter myWriter = new FileWriter(__INDEX_PATH__ + "infos.idx");
        myWriter.write("averageLength=" + averageLength + "\ntotalNumofArticles=" + totalNumofArticles);
        myWriter.close();

        long stopTime = System.nanoTime();
        long elapsedTime = stopTime - startTime;
        // Now we have finished creating the partial indexes
        // So we have to merge them (call merge())
        long startmerging = System.nanoTime();
        fastmerge(index.Voc_Path_queue, index.Pos_Path_queue, __INDEX_PATH__);
        long afterMergeStopTime = System.nanoTime();
        long afterMergeElapsedTime = afterMergeStopTime - startmerging;
        long startNormTime = System.nanoTime();
        System.out.println("Partial Creation Time:" + TimeUnit.SECONDS.convert(elapsedTime, TimeUnit.NANOSECONDS));
        System.out.println("Merging Partials Time:" + TimeUnit.SECONDS.convert(afterMergeElapsedTime, TimeUnit.NANOSECONDS));
        create_Norm_In_DocumentsFile();
        long afterNormStopTime = System.nanoTime();
        long afterNormElapsedTime = afterNormStopTime - startNormTime;
        long totalElapsedTime = elapsedTime + afterMergeElapsedTime + afterNormElapsedTime;

        System.out.println("Weights calculation Time:" + TimeUnit.SECONDS.convert(afterNormElapsedTime, TimeUnit.NANOSECONDS));
        System.out.println("Total Time:" + TimeUnit.SECONDS.convert(totalElapsedTime, TimeUnit.NANOSECONDS));

        System.out.println("articles " + totalNumofArticles);

        return false;
    }




    private void new_create_Norm_In_DocumentsFile(String docPath) throws IOException {
        BufferedReader voc = new BufferedReader(new FileReader(__INDEX_PATH__ + __VOCABULARY_FILENAME__));
        HashMap<String, Pair<Integer, Long>> vocabularyMap = new HashMap<>(); //<word, <df,pointer>>
        String input;
        while ((input = voc.readLine()) != null) {
            String[] array = input.split("\t");
            String term = array[0];
            int df = Integer.parseInt(array[1]);
            long pointer = Long.parseLong(array[2]);
            vocabularyMap.put(term, new Pair<>(df, pointer));
        }
        RandomAccessFile doc = new RandomAccessFile(__INDEX_PATH__ + __DOCUMENTS_FILENAME__, "rw");
        doc.seek(0);
        for (String path : absolutePaths) {
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();

            while (line != null) {
                double max_fieldfreq = 1;

                //-----auta tha ginon mpoun sto DocFile--------
                String str_docID = null;
                String str_title = null;
                ArrayList<String> auhtors = new ArrayList<>();
                ArrayList<String> authorsIDs = new ArrayList<>();
                String str_year = null;
                String str_journal = null;
                //--------------------------------------------


                S2TextualEntry textualEntry;

                textualEntry = S2JsonEntryReader.readTextualEntry(line);
                str_docID = textualEntry.getId();
                str_title = textualEntry.getTitle().replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();
                Return_Words_Map( new StringTokenizer(str_title));

                StringTokenizer abstract__st;
                abstract__st = new StringTokenizer(textualEntry.getPaperAbstract().replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(abstract__st);



                str_journal = textualEntry.getJournalName().replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();

                Return_Words_Map(new StringTokenizer(str_journal));

                StringTokenizer year__st;
                int year = textualEntry.getYear();
                str_year = Integer.toString(textualEntry.getYear());
                year__st = new StringTokenizer(str_year.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(year__st);

                StringTokenizer venue__st;
                venue__st = new StringTokenizer(textualEntry.getVenue().replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                Return_Words_Map(venue__st);

                for (String str : textualEntry.getEntities()) {
                    StringTokenizer entities__st;
                    entities__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(entities__st);
                }

                for (String str : textualEntry.getFieldsOfStudy()) {
                    StringTokenizer fieldsOfStudy__st;
                    fieldsOfStudy__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(fieldsOfStudy__st);
                }

                for (String str : textualEntry.getSources()) {
                    StringTokenizer sources__st;
                    sources__st = new StringTokenizer(str.replaceAll("[^a-zA-Z0-9]", " ").toLowerCase());
                    Return_Words_Map(sources__st);
                }

                List<Pair<String, List<String>>> authors = textualEntry.getAuthors();
                for (int i = 0; i < authors.size(); i++) {
                    String author = authors.get(i).getL();
                    StringTokenizer author__st;
                    String a = author.replaceAll("[^a-zA-Z0-9]", " ").replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();
                    auhtors.add(a);
                    for (int j = 0; j < authors.get(i).getR().size(); j++) {
                        String str = authors.get(i).getR().get(j).replaceAll("[^a-zA-Z0-9]", " ")
                                .replaceAll("[^\\p{L}\\p{Nd}\\p{Nl}]+", " ").toLowerCase();

                        authorsIDs.add(str);
                    }
                    author__st = new StringTokenizer(a);
                    Return_Words_Map(author__st);
                }


                HashMap<String,Double> mymap_Stemmed=new HashMap<>();
                for(String term:mymap.keySet()) {
                    String stemmed_term=Stemmer.Stem(term);
                    if(mymap_Stemmed.containsKey(stemmed_term)) {
                        double freq=mymap_Stemmed.get(stemmed_term);
                        freq += mymap.get(term);
                        mymap_Stemmed.put(stemmed_term,freq);
                        if(max_fieldfreq<freq) {
                            max_fieldfreq=freq;
                        }
                    }
                    else {
                        double freq=mymap.get(term);
                        mymap_Stemmed.put(stemmed_term,freq);
                        if(max_fieldfreq<freq){
                            max_fieldfreq=freq;
                        }
                    }
                }

                double Sum=0;
                for(String term:mymap_Stemmed.keySet()) {
                    double tf=mymap_Stemmed.get(term)/max_fieldfreq;
                    int df=vocabularyMap.get(term).getL();
                    double weight = weightCalculator(df, tf);
                    Sum += Math.pow(weight,2);
                }

                double sqrtSum=Math.sqrt(Sum);

                String docID2 = doc.readUTF(); //docid
                if(!docID2.equals(str_docID))    System.err.println("errrrrrror "+docID2);
                doc.readUTF(); //title
                doc.readUTF(); //authors
                doc.readUTF(); //authos id
                doc.readUTF(); //year
                doc.readUTF(); //journal
                String newValue;
                if (sqrtSum < 10) {
                    //1,23456789 1->1th byte ,->2nd byte 23456789->up to 10th byte
                    newValue = String.format("%.8f", sqrtSum);
                } else if (sqrtSum < 100) {
                    newValue = String.format("%.7f", sqrtSum);
                } else if (sqrtSum < 1000) {
                    newValue = String.format("%.6f", sqrtSum);
                } else {
                    newValue = String.format("%.5f", sqrtSum);
                }
                if ("1000000000".length() != newValue.length()) {
                    System.err.println("tha pethanoume oloi");
                }

                doc.writeUTF(newValue); //new norm

                doc.readUTF();//length
                doc.readUTF();//PageRank
                doc.readUTF();//authors
                doc.readUTF();//newline


                mymap.clear();
                line = reader.readLine();
            }
            reader.close();
        }






    }





    private void create_Norm_In_DocumentsFile() throws IOException {



        File vfile = new File(__INDEX_PATH__ + __VOCABULARY_FILENAME__);
        File pfile = new File(__INDEX_PATH__ + __POSTINGS_FILENAME__);
        File dfile = new File(__INDEX_PATH__+__DOCUMENTS_FILENAME__);
//        RandomAccessFile vocFile = new RandomAccessFile(vfile, "r");
        BufferedReader vocFile = new BufferedReader(new FileReader(vfile));
        RandomAccessFile postFile = new RandomAccessFile(pfile, "r");
        RandomAccessFile docFile = new RandomAccessFile(dfile, "rw");
        /*   edw tha ftiaksoyme ena Hashmap<String=docId,Double=Sum(weigts^2)>
         *    tha diavasoyme loipon olo to vocFile kai gua kathe leksh tha phaginoume sto postingFile
         *    sto block pou anaferetai. kai tha ypologizoume to varos ths lekshs gia kathe keimeno(docID) poy exei vrethei
         *    meta tha phaginoume sto hashmap sto value poy exei key=docid kai tha prosthetoume sto value to tetragwno tou
         *    varous moy exoyme molis upologisei.
         *    Otan teleiwsoume me to parsarisma tou vocID tha upologisoume th riza gia to value toy kathe key sto hashmap
         *    to opoio kai tha antistoixei sth norma tou docid kai tha to apothhkeusoume sto antistoixo pedio tou documentFile
         * */
        //System.out.println("here");
        HashMap<String, Double> normMap = new HashMap<>(); //docid,norm=metro tou dianusmatos tou docid
        String vocLine = vocFile.readLine();
        while (vocLine != null) {
            String[] vocInput = vocLine.split("\t");
//            vocInput[0]=term
//            vocInput[1]=df
//            vocInput[2]=pointer to PostingFIle
            int df = Integer.parseInt(vocInput[1]);
            long postingPointer = Long.parseLong(vocInput[2]);
            postFile.seek(postingPointer);
            byte[] b= new byte[56*df];
            postFile.read(b);
            int offset=0;
            for(int i=0;i<df;i++){
                byte[] id=Arrays.copyOfRange(b,offset,offset+40);
                offset+=40;
                byte[] _tf_=Arrays.copyOfRange(b,offset,offset+8);
                offset+=8;
                byte[] _pointer_=Arrays.copyOfRange(b,offset,offset+8);
                offset+=8;
                String docID=new String(id);
//                System.out.println("docid "+docID);
                double tf=ByteBuffer.wrap(_tf_).getDouble();
//                System.out.println("tf "+tf);
                long pointer=ByteBuffer.wrap(_pointer_).getLong();
//                System.out.println("pointer "+pointer);
                double weight = weightCalculator(df, tf);
                if (!normMap.containsKey(docID)) {
                    normMap.put(docID, Math.pow(weight, 2));
                } else {
                    double sum = normMap.get(docID);
                    sum = sum + Math.pow(weight, 2);
                    normMap.put(docID, sum);
                }

            }
            
            vocLine = vocFile.readLine();

        }
        postFile.close();
        vocFile.close();
        long offset=0;
        System.out.println("before normMap.size() : "+normMap.size());
        int counter=normMap.size();
        for (int i = 0; i < counter; i++) {
            byte[] docID=new byte[40];
            docFile.read(docID);
            offset+=40;
            String id=new String(docID);
            //System.out.println("docID : "+id);
            double sum = normMap.get(id);
            normMap.remove(id);
            double sqrtSum = Math.sqrt(sum);
            if(sqrtSum==0.0)    System.err.println("ERROR SQRT=0 FOR DOCID: "+id);
            docFile.writeDouble(sqrtSum);
            docFile.skipBytes(20);
            offset+=8; // wrote 8 bytes
            offset+=20; // skip length(int 4)-Pagerank(double 8)-AuthorsRank(double 8)

        }
        System.out.println("counter : "+counter);
        System.out.println("after normap.size() : "+normMap.size());
        System.out.println("offset : "+offset);
        System.out.println("docSize : "+docFile.length());
        docFile.close();

    }

    private double weightCalculator(int df, double tf) {
        //weight = tf * idf = tf * log2(N/df)
        double weight = 0.0;
        double idf = Math.log(totalNumofArticles / df) / Math.log(2);
        weight = tf * idf;
        return weight;
    }

    /*
     * new one level merge. All the partials in the same time. Estimated time approximately
     * equals to one level of the classic merges
     *
     */
    public void fastmerge(Queue<String> Voc_Path_queue, Queue<String> Pos_Path_queue, String __INDEX_PATH__) throws IOException {
        BufferedWriter vocFile = new BufferedWriter(new FileWriter(__INDEX_PATH__ + __VOCABULARY_FILENAME__, true));
        RandomAccessFile postFile = new RandomAccessFile(__INDEX_PATH__ + __POSTINGS_FILENAME__, "rw");
        BufferedReader[] vocabularies = new BufferedReader[Voc_Path_queue.size()];
        RandomAccessFile[] postings = new RandomAccessFile[Pos_Path_queue.size()];

//        System.out.println("voc Size "+vocabularies.length);
//        System.out.println("pos Size "+postings.length);

        int counter = 0;
        for (String path : Voc_Path_queue) {
//            System.out.println("counter= "+counter+" path: "+path);
            vocabularies[counter] = new BufferedReader(new FileReader(path));
            counter++;
        }
        counter = 0;
        for (String path : Pos_Path_queue) {
//            System.out.println("counter= "+counter+" path: "+path );
            postings[counter] = new RandomAccessFile(path, "r");
            counter++;
        }

        TreeMap<String, ArrayList<newPostingData>> wordsTree = new TreeMap<>();

        for (int i = 0; i < vocabularies.length; i++) {
            String line = vocabularies[i].readLine();
            String[] term = line.split("\t");
//            System.out.println(i+" "+line);
            if (wordsTree.containsKey(term[0])) {
                ArrayList<newPostingData> list = wordsTree.get(term[0]);
                newPostingData data = new newPostingData();
                data.setDf(Integer.parseInt(term[1]));
                data.setIndex(i);
                data.setPointer(Long.parseLong(term[2]));
                list.add(data);
                wordsTree.put(term[0], list);
            } else {
                ArrayList<newPostingData> list = new ArrayList<>();
                newPostingData data = new newPostingData();
                data.setDf(Integer.parseInt(term[1]));
                data.setIndex(i);
                data.setPointer(Long.parseLong(term[2]));
                list.add(data);
                wordsTree.put(term[0], list);
            }

        }
//        System.out.println("---------------------");
//        for(String s:wordsTree.keySet()){
//            System.out.println(s);
//            for(newPostingData ob:wordsTree.get(s)){
//                System.out.println("\t"+ob.getDf()+" "+ob.getIndex()+" "+ob.getPointer());
//            }
//        }

        long pointer_in_Posting = 0;
        while (!wordsTree.isEmpty()) {
            String firstWord = wordsTree.firstKey();
//            System.out.println("firstWord: "+firstWord);
            ArrayList<newPostingData> list = wordsTree.get(firstWord);
            wordsTree.remove(firstWord);
            int word_DF = 0;
            pointer_in_Posting = postFile.getFilePointer();
            for (newPostingData ob : list) {
                word_DF += ob.getDf();
                long pointer = ob.getPointer();
                int index = ob.getIndex();
                postings[index].seek(pointer);
                byte[] b= new byte[ob.getDf() * 56];
//                for (int i = 0; i < ob.getDf(); i++) {
//                    String str = postings[index].readUTF();
//                    System.out.println(str);
                    postings[index].read(b);
//                }
                postFile.write(b);
                String line = vocabularies[index].readLine();
                if (line != null) {
                    String[] term = line.split("\t");
//                    System.out.println("new added from " + index + " -> " + line);
                    if (wordsTree.containsKey(term[0])) {
                        ArrayList<newPostingData> l = wordsTree.get(term[0]);
                        newPostingData data = new newPostingData();
                        data.setDf(Integer.parseInt(term[1]));
                        data.setIndex(index);
                        data.setPointer(Long.parseLong(term[2]));
                        l.add(data);
                        wordsTree.put(term[0], l);
                    } else {
                        ArrayList<newPostingData> l = new ArrayList<>();
                        newPostingData data = new newPostingData();
                        data.setDf(Integer.parseInt(term[1]));
                        data.setIndex(index);
                        data.setPointer(Long.parseLong(term[2]));
                        l.add(data);
                        wordsTree.put(term[0], l);
                    }
                } else {
                    vocabularies[index].close();
                    postings[index].close();
                }

            }
            vocFile.append(firstWord + "\t" + word_DF + "\t" + pointer_in_Posting + "\n");

        }
        vocFile.close();
        postFile.close();

        for (String path : Voc_Path_queue) {
            File myObj = new File(path);
            if (myObj.delete()) {
                System.out.println("Deleted the file: " + myObj.getName());
            } else {
                System.out.println("Failed to delete the file.");
            }
        }
        Voc_Path_queue.clear();
        for (String path : Pos_Path_queue) {
            File myObj = new File(path);
            if (myObj.delete()) {
                System.out.println("Deleted the file: " + myObj.getName());
            } else {
                System.out.println("Failed to delete the file.");
            }
        }
        Pos_Path_queue.clear();
    }


    /**
     * Method that merges two partial indexes and creates a new index with ID
     * nextID, which is either a new partial index or the final index if we have
     * finished merging (i.e., if nextID = 0)
     * <p>
     * //@param partialID1
     * //@param partialID2
     * //@param nextID
     *
     * @return
     */

    public void newmerge(Queue<String> Voc_Path_queue, Queue<String> Pos_Path_queue, String __INDEX_PATH__) throws IOException {
        int _MAX_NUMBER_OF_RECORDS_ = 20000000;
        while (!Voc_Path_queue.isEmpty()) {
            String voc_path1 = Voc_Path_queue.remove();
            String pos_path1 = Pos_Path_queue.remove();
            if (!Voc_Path_queue.isEmpty()) {
                int _current_Number_of_records_ = 0;
                TreeMap<String, ArrayList<postData>> sortedTree = new TreeMap<>();
                String vocabulary_path;
                String posting_path;
                if (Voc_Path_queue.size() == 1) {
                    vocabulary_path = __VOCABULARY_FILENAME__;
                    posting_path = __POSTINGS_FILENAME__;
                } else {
                    vocabulary_path = Merged_Voc_File_Name_Creator();//kainourio vocFile gia na valoume ta merged
                    posting_path = Merged_Posting_File_Name_Creator();//kainourio postFile gia na valoume ta merged
                }
                String Path_name_vocabularyFile = __INDEX_PATH__ + vocabulary_path;
                Voc_Path_queue.add(Path_name_vocabularyFile);//to vazoyme sthn oura gia na to kanoyme kai ayto merge se epomeno stadio

                String Path_name_postingFile = __INDEX_PATH__ + posting_path;
                Pos_Path_queue.add(Path_name_postingFile);//omoiws me to vocfile

                File Vfile = new File(Path_name_vocabularyFile);
                File Pfile = new File(Path_name_postingFile);


                String voc_path2 = Voc_Path_queue.remove();
                String pos_path2 = Pos_Path_queue.remove();
                System.out.println("merging " + voc_path1 + " + " + voc_path2);


                BufferedReader oldvoc1 = new BufferedReader(new FileReader(voc_path1));
                BufferedReader oldvoc2 = new BufferedReader(new FileReader(voc_path2));

                String inputVoc1 = oldvoc1.readLine();
                String inputVoc2 = oldvoc2.readLine();
                System.out.println("start reading voc1");
                while ((inputVoc1 != null) && (inputVoc2 != null)) {
                    String[] voc1_words = inputVoc1.split("\t");
                    String[] voc2_words = inputVoc2.split("\t");
                    int res = voc1_words[0].compareTo(voc2_words[0]);
                    if (res == 0) {//words are equals
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(1, Long.parseLong(voc1_words[2]), Integer.parseInt(voc1_words[1])));
                        _current_Number_of_records_++;
                        tmp.add(new postData(2, Long.parseLong(voc2_words[2]), Integer.parseInt(voc2_words[1])));
                        sortedTree.put(voc1_words[0], tmp);
                        _current_Number_of_records_++;
                        inputVoc1 = oldvoc1.readLine();
                        inputVoc2 = oldvoc2.readLine();

                    } else if (res > 0) { //insert word of voc2
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(2, Long.parseLong(voc2_words[2]), Integer.parseInt(voc2_words[1])));
                        sortedTree.put(voc2_words[0], tmp);
                        _current_Number_of_records_++;
                        inputVoc2 = oldvoc2.readLine();
                    } else {
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(1, Long.parseLong(voc1_words[2]), Integer.parseInt(voc1_words[1])));
                        sortedTree.put(voc1_words[0], tmp);
                        _current_Number_of_records_++;
                        inputVoc1 = oldvoc1.readLine();
                    }
                    if (_current_Number_of_records_ >= _MAX_NUMBER_OF_RECORDS_ || ((inputVoc1 == null) && (inputVoc2 == null))) {
                        _current_Number_of_records_ = 0;
                        write_to_merged_Voc_and_Pos(sortedTree, Vfile, Pfile, pos_path1, pos_path2);
                        sortedTree.clear();
                    }

                }
                if (inputVoc1 == null && inputVoc2 == null) {
                    System.out.println("empty stage");
                } else if (inputVoc1 == null && inputVoc2 != null) {
                    while (inputVoc2 != null) {
                        String[] voc2_words = inputVoc2.split("\t");
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(2, Long.parseLong(voc2_words[2]), Integer.parseInt(voc2_words[1])));
                        sortedTree.put(voc2_words[0], tmp);
                        _current_Number_of_records_++;
                        inputVoc2 = oldvoc2.readLine();
                        if (_current_Number_of_records_ >= _MAX_NUMBER_OF_RECORDS_ || inputVoc2 == null) {
                            _current_Number_of_records_ = 0;
                            write_to_merged_Voc_and_Pos(sortedTree, Vfile, Pfile, pos_path1, pos_path2);
                            sortedTree.clear();
                        }
                    }

                } else if (inputVoc1 != null && inputVoc2 == null) {
                    while (inputVoc1 != null) {
                        String[] voc1_words = inputVoc1.split("\t");
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(1, Long.parseLong(voc1_words[2]), Integer.parseInt(voc1_words[1])));
                        sortedTree.put(voc1_words[0], tmp);
                        _current_Number_of_records_++;
                        inputVoc1 = oldvoc1.readLine();
                        if (_current_Number_of_records_ >= _MAX_NUMBER_OF_RECORDS_ || inputVoc1 == null) {
                            _current_Number_of_records_ = 0;
                            write_to_merged_Voc_and_Pos(sortedTree, Vfile, Pfile, pos_path1, pos_path2);
                            sortedTree.clear();
                        }
                    }
                } else {
                    System.err.println("this is a case should not happen! it happens" +
                            "because while stopped but neither voc1_line nor voc2_line is null");
                }
                oldvoc1.close();
                oldvoc2.close();

                File myObj = new File(voc_path1);
                if (myObj.delete()) {
                    System.out.println("Deleted the file: " + myObj.getName());
                } else {
                    System.out.println("Failed to delete the file.");
                }
                myObj = new File(voc_path2);
                if (myObj.delete()) {
                    System.out.println("Deleted the file: " + myObj.getName());
                } else {
                    System.out.println("Failed to delete the file.");
                }

            }
        }

    }

    public void write_to_merged_Voc_and_Pos(TreeMap<String, ArrayList<postData>> sortedTree, File mergedVoc, File mergedPost,
                                            String oldPost1, String oldPost2) throws IOException {

        FileWriter m_vocFile = new FileWriter(mergedVoc, true);
        BufferedWriter merged_VocFile = new BufferedWriter(m_vocFile);
        RandomAccessFile merged_PostFile = new RandomAccessFile(mergedPost, "rw");

        RandomAccessFile oldpos1 = new RandomAccessFile(oldPost1, "r");
        RandomAccessFile oldpos2 = new RandomAccessFile(oldPost2, "r");


        for (String word : sortedTree.keySet()) {
            int df = 0;
            //edw tha arxisei na grafei to block ths word
            long keeppointer = merged_PostFile.getFilePointer();

            for (postData data : sortedTree.get(word)) {
                df += data.getDf();
                if (data.getVoc() == 1) {
                    oldpos1.seek(data.getPointer());
                    for (int i = 0; i < data.getDf(); i++) {

                        String line = oldpos1.readUTF();

                        merged_PostFile.writeUTF(line);

//                                merged_PostFile_seek=merged_PostFile.getFilePointer();

                    }
                } else {
                    oldpos2.seek(data.getPointer());
                    for (int i = 0; i < data.getDf(); i++) {
                        //merged_PostFile.seek(merged_PostFile_seek);
                        merged_PostFile.writeUTF(oldpos2.readUTF());
//                                merged_PostFile_seek=merged_PostFile.getFilePointer();
                    }
                }

            }

            merged_VocFile.append(word + "\t" + df + "\t" + keeppointer + "\n");

        }
        merged_PostFile.close();
        merged_VocFile.close();
        oldpos1.close();
        oldpos2.close();
        File myObj = new File(oldPost1);
        if (myObj.delete()) {
            System.out.println("Deleted the file: " + myObj.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }
        myObj = new File(oldPost2);
        if (myObj.delete()) {
            System.out.println("Deleted the file: " + myObj.getName());
        } else {
            System.out.println("Failed to delete the file.");
        }


    }


    public void merge(Queue<String> Voc_Path_queue, Queue<String> Pos_Path_queue, String __INDEX_PATH__) throws IOException {
        while (!Voc_Path_queue.isEmpty()) {
            String voc_path1 = Voc_Path_queue.remove();
            String pos_path1 = Pos_Path_queue.remove();
            if (!Voc_Path_queue.isEmpty()) {
                TreeMap<String, ArrayList<postData>> sortedTree = new TreeMap<>();
                String vocabulary_path;
                String posting_path;
                if (Voc_Path_queue.size() == 1) {
                    vocabulary_path = __VOCABULARY_FILENAME__;
                    posting_path = __POSTINGS_FILENAME__;
                } else {
                    vocabulary_path = Merged_Voc_File_Name_Creator();//kainourio vocFile gia na valoume ta merged
                    posting_path = Merged_Posting_File_Name_Creator();//kainourio postFile gia na valoume ta merged
                }
                String Path_name_vocabularyFile = __INDEX_PATH__ + vocabulary_path;
                Voc_Path_queue.add(Path_name_vocabularyFile);//to vazoyme sthn oura gia na to kanoyme kai ayto merge se epomeno stadio

                String Path_name_postingFile = __INDEX_PATH__ + posting_path;
                Pos_Path_queue.add(Path_name_postingFile);//omoiws me to vocfile

                File Vfile = new File(Path_name_vocabularyFile);
                File Pfile = new File(Path_name_postingFile);


                String voc_path2 = Voc_Path_queue.remove();
                String pos_path2 = Pos_Path_queue.remove();
                System.out.println("merging " + voc_path1 + " + " + voc_path2);


                BufferedReader oldvoc1 = new BufferedReader(new FileReader(voc_path1));
                BufferedReader oldvoc2 = new BufferedReader(new FileReader(voc_path2));

                String inputVoc1;
                String inputVoc2;
                System.out.println("start reading voc1");
                while ((inputVoc1 = oldvoc1.readLine()) != null) {
                    //System.out.println(inputVoc1);
                    String[] voc_words = inputVoc1.split("\t");
                    ArrayList<postData> tmp = new ArrayList<>();

                    tmp.add(new postData(1, Long.parseLong(voc_words[2]), Integer.parseInt(voc_words[1])));
                    sortedTree.put(voc_words[0], tmp);
                }
                System.out.println("end of voc1");
                System.out.println("start reading voc2");
                while ((inputVoc2 = oldvoc2.readLine()) != null) {
                    String[] voc_words = inputVoc2.split("\t");
                    if (!sortedTree.containsKey(voc_words[0])) {
                        ArrayList<postData> tmp = new ArrayList<>();
                        tmp.add(new postData(2, Long.parseLong(voc_words[2]), Integer.parseInt(voc_words[1])));
                        sortedTree.put(voc_words[0], tmp);
                    } else {
                        ArrayList<postData> tmp = sortedTree.get(voc_words[0]);
                        tmp.add(new postData(2, Long.parseLong(voc_words[2]), Integer.parseInt(voc_words[1])));
                        sortedTree.put(voc_words[0], tmp);
                    }
                }
                System.out.println("end of voc2");

                oldvoc1.close();
                oldvoc2.close();

                RandomAccessFile oldpos1 = new RandomAccessFile(pos_path1, "r");
                RandomAccessFile oldpos2 = new RandomAccessFile(pos_path2, "r");

                FileWriter m_vocFile = new FileWriter(Vfile, true);
                BufferedWriter merged_VocFile = new BufferedWriter(m_vocFile);

                RandomAccessFile merged_PostFile = new RandomAccessFile(Pfile, "rw");
                long merged_PostFile_seek = 0;
                System.out.println();
                System.out.println("start of sortedtree");
                for (String word : sortedTree.keySet()) {
                    int df = 0;
                    //edw tha arxisei na grafei to block ths word
                    long keeppointer = merged_PostFile.getFilePointer();

                    for (postData data : sortedTree.get(word)) {
                        df += data.getDf();
                        if (data.getVoc() == 1) {
                            oldpos1.seek(data.getPointer());
                            for (int i = 0; i < data.getDf(); i++) {

                                String line = oldpos1.readUTF();

                                merged_PostFile.writeUTF(line);

//                                merged_PostFile_seek=merged_PostFile.getFilePointer();

                            }
                        } else {
                            oldpos2.seek(data.getPointer());
                            for (int i = 0; i < data.getDf(); i++) {
                                //merged_PostFile.seek(merged_PostFile_seek);
                                merged_PostFile.writeUTF(oldpos2.readUTF());
//                                merged_PostFile_seek=merged_PostFile.getFilePointer();
                            }
                        }

                    }

                    merged_VocFile.append(word + "\t" + df + "\t" + keeppointer + "\n");

                }
                merged_PostFile.close();
                merged_VocFile.close();
                oldpos1.close();
                oldpos2.close();
                System.out.println("end of merging");
            }
        }
    }


    public void oldmerge(Queue<String> Voc_Path_queue, Queue<String> Pos_Path_queue, String __INDEX_PATH__) throws IOException {
        // Read vocabulary files line by line in corresponding dirs
        // and check which is the shortest lexicographically.
        // Read the corresponding entries in the postings and documents file
        // and append accordingly the new ones
        // If both partial indexes contain the same word, them we have to update
        // the df and append the postings and documents of both
        // Continue with the next lexicographically shortest word
        // Dump the new index and delete the old partial indexes

        // If nextID = 0 (i.e., we have finished merging partial indexes, store
        // all idx files to INDEX_PATH

        while (!Voc_Path_queue.isEmpty()) {
            //pare to prwto pou mphke kai afairesai to
            String voc_path1 = Voc_Path_queue.remove();
            String pos_path1 = Pos_Path_queue.remove();
            if (!Voc_Path_queue.isEmpty()) { //an exei kai allo shmainei pvw tha prpei na kanoyme merge duo alliws einai
                //to totalmerged auto
                String vocabulary_path;
                String posting_path;
                if (Voc_Path_queue.size() == 1) {
                    vocabulary_path = __VOCABULARY_FILENAME__;
                    posting_path = __POSTINGS_FILENAME__;
                } else {
                    vocabulary_path = Merged_Voc_File_Name_Creator();//kainourio vocFile gia na valoume ta merged
                    posting_path = Merged_Posting_File_Name_Creator();//kainourio postFile gia na valoume ta merged
                }

                String Path_name_vocabularyFile = __INDEX_PATH__ + vocabulary_path;
                Voc_Path_queue.add(Path_name_vocabularyFile);//to vazoyme sthn oura gia na to kanoyme kai ayto merge se epomeno stadio

                String Path_name_postingFile = __INDEX_PATH__ + posting_path;
                Pos_Path_queue.add(Path_name_postingFile);//omoiws me to vocfile

                File Vfile = new File(Path_name_vocabularyFile);
                File Pfile = new File(Path_name_postingFile);
                RandomAccessFile merged_VocFile = new RandomAccessFile(Vfile, "rw");
                RandomAccessFile merged_PostFile = new RandomAccessFile(Pfile, "rw");
                //arxikopoioume ekw apo th while gia na kratame pou vriskete to seek sta kainouria arxeia
                //kathe fora
                long merged_PostFile_seek = 0;
                long merged_VocFile_seek = 0;
                //prin kanoume gia prwth fora write tou leme oti tha prepei na grapsei sth thesh 0
                //omoiws me pio panw
                String voc_path2 = Voc_Path_queue.remove();
                System.out.println("merging " + voc_path1 + " + " + voc_path2);
                String pos_path2 = Pos_Path_queue.remove();
                RandomAccessFile oldvoc1 = new RandomAccessFile(voc_path1, "r");
                RandomAccessFile oldvoc2 = new RandomAccessFile(voc_path2, "r");
                RandomAccessFile oldpos1 = new RandomAccessFile(pos_path1, "r");
                RandomAccessFile oldpos2 = new RandomAccessFile(pos_path2, "r");
                String inputVoc1 = oldvoc1.readLine();
                String inputVoc2 = oldvoc2.readLine();


                while (inputVoc1 != null && inputVoc2 != null) {
                    //System.out.println("stage 1");
                    //System.out.println(voc1_line+" vs "+voc2_line);
                    String[] voc1_words = inputVoc1.split("\t");
                    String[] voc2_words = inputVoc2.split("\t");
                    int res = voc1_words[0].compareTo(voc2_words[0]);
                    if (res == 0) {
                        //System.out.println("res==0");
                        //einai oi idies lexeis
                        //neo df pou prokuptei apo to sum twn paliwn
                        int old_df1 = Integer.parseInt(voc1_words[1]);
                        int old_df2 = Integer.parseInt(voc2_words[1]);
                        int df = old_df1 + old_df2;
//                        counter+=df;
//                        cnt_equals++;
//                        System.out.println(voc1_words[0]+" , "+voc1_words[1]+" vs "+voc2_words[0]+" , "+voc2_words[1]);
//                        //autoi einai oi pointers apo to kathe voc sto antistoixo post gia th lexi
                        long post1_pointer = Long.parseLong(voc1_words[2]);
                        long post2_pointer = Long.parseLong(voc2_words[2]);
                        //kinoume mesa sto kainoyrio vocfile
                        merged_VocFile.seek(merged_VocFile_seek);
                        merged_VocFile.write((voc1_words[0] + "\t" + df + "\t" + merged_PostFile_seek + "\n").getBytes("UTF-8"));
                        merged_VocFile_seek = merged_VocFile.getFilePointer();
                        oldpos1.seek(post1_pointer);
                        for (int i = 0; i < old_df1; i++) {
                            String newLine = oldpos1.readLine() + "\n";
                            merged_PostFile.seek(merged_PostFile_seek);
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        oldpos2.seek(post2_pointer);
                        for (int i = 0; i < old_df2; i++) {
                            String newLine = oldpos2.readLine() + "\n";
                            merged_PostFile.seek(merged_PostFile_seek);
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        //pairnw tis nees times twn seek gia na kserw pou na paw sthn epomenh loop
                        //merged_PostFile_seek = merged_PostFile.getFilePointer();
                        inputVoc1 = oldvoc1.readLine();
                        inputVoc2 = oldvoc2.readLine();
                    } else if (res > 0) {
                        //System.out.println("res>0");
                        //edw tha kataxvrhsoyme th lexh apo to w2 kai tha prepei na krathsoume
                        //th lexh apo to voc1 gia na th sugkrinoume me thn epomenh
                        long post2_pointer = Long.parseLong(voc2_words[2]);
                        int old_df2 = Integer.parseInt(voc2_words[1]);
                        merged_VocFile.seek(merged_VocFile_seek);
                        merged_VocFile.write((voc2_words[0] + "\t" + old_df2 + "\t" + merged_PostFile_seek + "\n").getBytes("UTF-8"));
                        merged_VocFile_seek = merged_VocFile.getFilePointer();
                        oldpos2.seek(post2_pointer);
                        for (int i = 0; i < old_df2; i++) {
                            merged_PostFile.seek(merged_PostFile_seek);
                            String newLine = oldpos2.readLine() + "\n";
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        inputVoc2 = oldvoc2.readLine();
                    } else {
                        //System.out.println("res<0");
                        //a.compare(b) to a prepei na einai prin to b
                        //edw tha kataxvrhsoyme th lexh apo to w1 kai tha prepei na krathsoume
                        //th lexh apo to voc2 gia na th sugkrinoume me thn epomenh tou voc1
//                        System.out.println(voc1_words[0]);
                        long post1_pointer = Long.parseLong(voc1_words[2]);
                        int old_df1 = Integer.parseInt(voc1_words[1]);
                        merged_VocFile.seek(merged_VocFile_seek);
                        merged_VocFile.write((voc1_words[0] + "\t" + old_df1 + "\t" + merged_PostFile_seek + "\n").getBytes("UTF-8"));
                        merged_VocFile_seek = merged_VocFile.getFilePointer();
                        oldpos1.seek(post1_pointer);
                        for (int i = 0; i < old_df1; i++) {
                            merged_PostFile.seek(merged_PostFile_seek);
                            String newLine = oldpos1.readLine() + "\n";
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        inputVoc1 = oldvoc1.readLine();
                    }
                }
                if (inputVoc1 == null && inputVoc2 == null) {
                    System.out.println("empty stage");
                } else if (inputVoc1 == null && inputVoc2 != null) {
                    System.out.println("stage 2");
                    //fortwnw ola tis lexeis poy menoyn sto voc2_line
                    while (inputVoc2 != null) {
                        String voc2_line = inputVoc2;
                        String[] voc2_words = voc2_line.split("\t");
                        long post2_pointer = Long.parseLong(voc2_words[2]);
                        int old_df2 = Integer.parseInt(voc2_words[1]);
                        merged_VocFile.seek(merged_VocFile_seek);
                        merged_VocFile.write((voc2_words[0] + "\t" + voc2_words[1] + "\t" + merged_PostFile_seek + "\n").getBytes("UTF-8"));
                        merged_VocFile_seek = merged_VocFile.getFilePointer();
                        oldpos2.seek((post2_pointer));
                        for (int i = 0; i < old_df2; i++) {
                            merged_PostFile.seek(merged_PostFile_seek);
                            String newLine = oldpos2.readLine();
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        inputVoc2 = oldvoc2.readLine();
                    }
                } else if (inputVoc1 != null && inputVoc2 == null) {
                    System.out.println(" stage 3");
                    while (inputVoc1 != null) {
                        String voc1_line = inputVoc1;
                        String[] voc1_words = voc1_line.split("\t");
                        System.out.println(voc1_line);
                        long post1_pointer = Long.parseLong(voc1_words[2]);
                        int old_df1 = Integer.parseInt(voc1_words[1]);
                        merged_VocFile.seek(merged_VocFile_seek);
                        merged_VocFile.write((voc1_words[0] + "\t" + voc1_words[1] + "\t" + merged_PostFile_seek + "\n").getBytes("UTF-8"));
                        merged_VocFile_seek = merged_VocFile.getFilePointer();
                        oldpos1.seek((post1_pointer));
                        for (int i = 0; i < old_df1; i++) {
                            merged_PostFile.seek(merged_PostFile_seek);
                            String newLine = oldpos1.readLine() + "\n";
                            merged_PostFile.write(newLine.getBytes("UTF-8"));
                            merged_PostFile_seek = merged_PostFile.getFilePointer();
                        }
                        inputVoc1 = oldvoc1.readLine();
                    }
                    System.out.println("stage 3 over");
                } else {
                    System.err.println("this is a case should not happen! it happens" +
                            "because while stopped but neither voc1_line nor voc2_line is null");
                }
                merged_VocFile.close();
                merged_PostFile.close();
            } else {

                String[] ret = new String[2];
                ret[0] = voc_path1;
                ret[1] = pos_path1;

            }


        }

    }


    /**
     * Method that indexes the collection that is given in the themis.config
     * file
     * <p>
     * Used for the task of indexing!
     *
     * @return
     * @throws IOException
     */
    public boolean index() throws IOException {
        String collectionPath = __CONFIG__.getDatasetPath();
        if (collectionPath != null) {
            return index(collectionPath);
        } else {
            __LOGGER__.error("DATASET_PATH not set in themis.config!");
            return false;
        }
    }

    /**
     * Method responsible for loading vocabulary file to memory and also opening
     * RAF files to postings and documents, ready to seek
     * <p>
     * Used for the task of querying!
     *
     * @return
     * @throws IOException
     */
    public boolean load() throws IOException {
        System.out.println("Loading Vocabulary...");
        if (!hasIndex()) {
            __LOGGER__.error("Index is not constructed correctly!");
            return false;
        }
//        private HashMap<String, Pair<Integer, Long>> __VOCABULARY__ = null;
//        private RandomAccessFile __POSTINGS__ = null;
//        private RandomAccessFile __DOCUMENTS__ = null;
        __DOCUMENTS__ = new RandomAccessFile(__INDEX_PATH__ + __DOCUMENTS_FILENAME__, "r");
        __POSTINGS__ = new RandomAccessFile(__INDEX_PATH__ + __POSTINGS_FILENAME__, "r");
        BufferedReader voc = new BufferedReader(new FileReader(__INDEX_PATH__ + __VOCABULARY_FILENAME__));
        __VOCABULARY__ = new HashMap<>(); //<word, <df,pointer>>
        String input;
        while ((input = voc.readLine()) != null) {
            String[] array = input.split("\t");
            String term = array[0];
            int df = Integer.parseInt(array[1]);
            long pointer = Long.parseLong(array[2]);
            __VOCABULARY__.put(term, new Pair<>(df, pointer));
        }
        System.out.println("END OF LOADING....");

        voc.close();

//        HashSet<String> docs=new HashSet<>();
//        int counter=0;
//        int errors=0;
//        for(String word:__VOCABULARY__.keySet()){
//            Pair<Integer, Long> pair = __VOCABULARY__.get(word);
//            __POSTINGS__.seek(pair.getR());
//            byte[] b= new byte[56*pair.getL()];
//            __POSTINGS__.read(b);
//            int offset=0;
//            for (int i = 0; i < pair.getL(); i++) {
//                byte[] id = Arrays.copyOfRange(b, offset, offset + 40);
//                offset += 40;
//                byte[] _tf_ = Arrays.copyOfRange(b, offset, offset + 8);
//                offset += 8;
//                byte[] _pointer_ = Arrays.copyOfRange(b, offset, offset + 8);
//                offset += 8;
//                String pcid = new String(id);
//                long pointer = ByteBuffer.wrap(_pointer_).getLong();
//                if (!docs.contains(pcid)) {
//
//                    __DOCUMENTS__.seek(pointer);
//                    byte[] d = new byte[40];
//                    byte[] n = new byte[8];
//                    byte[] l = new byte[4];
//                    byte[] r = new byte[8];
//                    byte[] a = new byte[8];
//
//                    __DOCUMENTS__.read(d);
//                    __DOCUMENTS__.read(n);
//                    __DOCUMENTS__.read(l);
//                    __DOCUMENTS__.read(r);
//                    __DOCUMENTS__.read(a);
//                    if (pcid.equals(new String(d))) {
//                        docs.add(pcid);
//                        double norma = ByteBuffer.wrap(n).getDouble();
//                        int length_ = ByteBuffer.wrap(l).getInt();
//                        double rank = ByteBuffer.wrap(r).getDouble();
//                        double author = ByteBuffer.wrap(a).getDouble();
//                        if(norma<=0.0)  System.err.println( pcid+" "+norma+" "+length_+" "+rank+" "+author);
////                        else    System.out.println( pcid+" "+norma+" "+length_+" "+rank+" "+author);
//                        counter++;
//                        //System.out.println("counter: " +counter++);
//                    } else {
//                        //System.err.println("errors: "+ errors++);
//                        errors++;
//                        System.err.println("ERROR IN CHECK " + pcid + " is != " + new String(d)+" , "+pointer+" mod 68 ->"+pointer%68);
//                    }
//                }
//            }
//        }
//        System.out.println("counter : "+counter+"\n erors : "+errors);
//        //System.exit(0);
//        __DOCUMENTS__.seek(0);
//        for(int i=0;i<46947044;i++){
//            byte[] buffer=new byte[68];
//            __DOCUMENTS__.read(buffer);
//            int offset=0;
//            byte[] _docid_=Arrays.copyOfRange(buffer, offset, offset + 40);
//            offset+=40;
//            byte[] _norm_=Arrays.copyOfRange(buffer, offset, offset + 8);
//            offset+=8;
//            byte[] _length_=Arrays.copyOfRange(buffer, offset, offset + 4);
//            offset+=4;
//            byte[] _pageRank_=Arrays.copyOfRange(buffer, offset, offset + 8);
//            offset+=8;
//            byte[] _authorsRank_=Arrays.copyOfRange(buffer, offset, offset + 8);
//            offset+=8;
//
//            String docid=new String(_docid_);
//            double norm = ByteBuffer.wrap(_norm_).getDouble();
//            int length = ByteBuffer.wrap(_length_).getInt();
//            double pagerank = ByteBuffer.wrap(_pageRank_).getDouble();
//            double authorRank=ByteBuffer.wrap(_authorsRank_).getDouble();
//            if(norm<=0.0){
//                System.err.println("ERROS IN CHECH 2 "+docid+" "+norm+" "+length+" "+pagerank+" "+authorRank);
//            }
//            else{
//                System.out.println("-->"+docid+" "+norm+" "+length+" "+pagerank+" "+authorRank);
//            }
//            break;
//        }
//        System.out.println("END OF CHECKS");
        // Else load vocabulary file in memory in a HashMap and open
        // indexes postings and documents RAF files
        return false;
    }

    /**
     * Basic method for querying functionality. Given the list of terms in the
     * query, returns a List of Lists of DocInfoEssential objects, where each
     * list of DocInfoEssential objects holds where each list of
     * DocInfoEssential objects holds the DocInfoEssential representation of the
     * docs that the corresponding term of the query appears in. A
     * DocInfoEssential, should hold all needed information for implementing a
     * retrieval model, like VSM, Okapi-BM25, etc. This is more memory efficient
     * than holding getDocInfoFullTerms objects
     *
     * @param terms
     * @return
     */
    public static List<List<DocInfoEssential>> getDocInfoEssentialForTerms(List<String> terms) throws IOException {
        // If indexes are not loaded

        if (!loaded()) {
            System.out.println("it isn't loaded");
            return null;
        } else {
            List<List<DocInfoEssential>> bigList = new LinkedList<>();
            TreeMap<Long, HashMap<Long,Norm_length_PageRank>> BigHashMap=new TreeMap<>();
            for (String term : terms) {
                List<DocInfoEssential> smallList = new LinkedList<>();
                if (__VOCABULARY__.containsKey(term)) {
                    Pair<Integer, Long> pair = __VOCABULARY__.get(term);
                    __POSTINGS__.seek(pair.getR());
                    byte[] b= new byte[56*pair.getL()];
                    __POSTINGS__.read(b);
                    int offset=0;
                    for (int i = 0; i < pair.getL(); i++) {
                        byte[] id=Arrays.copyOfRange(b,offset,offset+40);
                        offset+=40;
                        byte[] _tf_=Arrays.copyOfRange(b,offset,offset+8);
                        offset+=8;
                        byte[] _pointer_=Arrays.copyOfRange(b,offset,offset+8);
                        offset+=8;
                        String pcid=new String(id);
                        double tf=ByteBuffer.wrap(_tf_).getDouble();
                        long pointer=ByteBuffer.wrap(_pointer_).getLong();
                        DocInfoEssential ob = new DocInfoEssential(pcid, pointer);
                        long number_of_block= pointer/_MAX_NUMBER_OF_BYTES_PER_BLOCK_;
                        if(BigHashMap.containsKey(number_of_block)){
                            if(!BigHashMap.get(number_of_block).containsKey(pointer)){
                                BigHashMap.get(number_of_block).put(pointer,new Norm_length_PageRank());
                            }
                        }
                        else{
                            HashMap<Long,Norm_length_PageRank> temp=new HashMap<>();
                            temp.put(pointer,new Norm_length_PageRank());
                            BigHashMap.put(number_of_block,temp);
                        }
//                        __DOCUMENTS__.seek(pointer+40);
//
//                        byte[] w=new byte[8];
//                        __DOCUMENTS__.read(w);
//                        double weight = ByteBuffer.wrap(b).getDouble();
//
//                        byte[] l=new byte[4];
//                        __DOCUMENTS__.read(l);
//                        int length = ByteBuffer.wrap(l).getInt();
//
//                        byte[] pr=new byte[8];
//                        __DOCUMENTS__.read(pr);
//                        double pageRank = ByteBuffer.wrap(b).getDouble();
//
//                        DocInfoEssential ob = new DocInfoEssential(pcid, pointer);
//                        //ob.getOffset()
//                        ob.setProperty(DocInfoEssential.PROPERTY.LENGTH, length);
//                        ob.setProperty(DocInfoEssential.PROPERTY.PAGERANK, pageRank);
//                        ob.setProperty(DocInfoEssential.PROPERTY.WEIGHT, weight);
                        smallList.add(ob);
                    }
                    bigList.add(smallList);
                }
            }
            System.out.println("number of block "+BigHashMap.size());
            for(Long number_of_block:BigHashMap.keySet()){
                long pointer_to_seek=_MAX_NUMBER_OF_BYTES_PER_BLOCK_*number_of_block;
//                System.out.println("pointer to seek: "+pointer_to_seek+" goes to ->"+(pointer_to_seek+_MAX_NUMBER_OF_BYTES_PER_BLOCK_));
//                System.out.println("__documents__.length(): "+__DOCUMENTS__.length());
//                System.out.println("(int)_MAX_NUMBER_OF_BYTES_PER_BLOCK_ : "+(int)_MAX_NUMBER_OF_BYTES_PER_BLOCK_);
                byte[] buffer=new byte[(int)_MAX_NUMBER_OF_BYTES_PER_BLOCK_];
//                System.out.println("buffer.length(): "+buffer.length);
                __DOCUMENTS__.seek(pointer_to_seek); //phgainoume sthn arxh tou block
                __DOCUMENTS__.read(buffer); // fortwnoume ston buffer ena block

                for(long pointer:BigHashMap.get(number_of_block).keySet()){

                    int offset=(int) (pointer % _MAX_NUMBER_OF_BYTES_PER_BLOCK_);

                    if( (offset+68) < _MAX_NUMBER_OF_BYTES_PER_BLOCK_ ) { //68 einai ta bytes tou kathe docid+norm+
                                                                          //length+pagerank+authorsrank sto Document
                        byte[] _docID_= Arrays.copyOfRange(buffer, offset, offset + 40);
                        offset += 40; //skip docID bytes
                        byte[] _norm_ = Arrays.copyOfRange(buffer, offset, offset + 8); //copy norm bytes
                        offset += 8; //skip norm bytes
                        byte[] _length_ = Arrays.copyOfRange(buffer, offset, offset + 4); //copy length bytes
                        offset += 4; //skip length bytes
                        byte[] _pagerank_ = Arrays.copyOfRange(buffer, offset, offset + 8); //copy pagerank bytes
                        offset += 8;
                        byte[] _authorsRank_ = Arrays.copyOfRange(buffer, offset, offset + 8); //copy pagerank bytes
                        offset += 8;
                        double norm = ByteBuffer.wrap(_norm_).getDouble();
                        if(norm==0.0) {

                            System.err.println("1. norm0.tostring is " + _norm_.toString() + " -> " + norm + " ,pointer: " + pointer + " ,offset: " + offset+" ,docID :"+_docID_.toString()+"  ");
                            System.err.println("number_of_block : "+number_of_block);
//                            System.err.print("Buffer contains : ");
//                            for(int i=0;i<buffer.length;i++){
//                                System.err.print(buffer[i]);
//                            }
                            System.err.println();
                            System.err.print("_docID_ : ");
                            for(int i=0;i<_docID_.length;i++){
                                System.err.print(_docID_[i]);
                            }
                            System.err.println(" -> "+new String(_docID_));
                            System.err.print("_norm_ : ");
                            for(int i=0;i<_norm_.length;i++){
                                System.err.print(_norm_[i]);
                            }
                            System.err.println(" -> "+norm);

                        }
//                        else {
//                            System.out.println("1. norm is "+_norm_.toString()+" -> "+norm+" - "+new String(_docID_)+"\nnumber of blocks : "+number_of_block);
//
//                        }
                        int length = ByteBuffer.wrap(_length_).getInt();
                        double pagerank = ByteBuffer.wrap(_pagerank_).getDouble();
                        BigHashMap.get(number_of_block).get(pointer).setLength(length);
                        BigHashMap.get(number_of_block).get(pointer).setNorm(norm);
                        BigHashMap.get(number_of_block).get(pointer).setPageRank(pagerank);
                    }
                    else{
                        //se auth th periptwsh to record toy docid exei moiraste se dyo blocks
                        //opote phgainoyme kai to kateutheian apo ton pointer me seek
                        //System.out.println("pointer -> "+pointer);
                        __DOCUMENTS__.seek(pointer);
                        offset=0;
                        byte[] small_buffer=new byte[68];
                        __DOCUMENTS__.read(small_buffer);
                        offset += 40; //skip docID bytes
                        byte[] _norm_ = Arrays.copyOfRange(small_buffer, offset, offset + 8); //copy norm bytes
                        offset += 8; //skip norm bytes
                        byte[] _length_ = Arrays.copyOfRange(small_buffer, offset, offset + 4); //copy length bytes
                        offset += 4; //skip length bytes
                        byte[] _pagerank_ = Arrays.copyOfRange(small_buffer, offset, offset + 8); //copy pagerank bytes

                        double norm = ByteBuffer.wrap(_norm_).getDouble();
                        if(norm==0.0)   System.err.println("2. norm is "+_norm_.toString());
                        int length = ByteBuffer.wrap(_length_).getInt();
                        double pagerank = ByteBuffer.wrap(_pagerank_).getDouble();
                        BigHashMap.get(number_of_block).get(pointer).setLength(length);
                        BigHashMap.get(number_of_block).get(pointer).setNorm(norm);
                        BigHashMap.get(number_of_block).get(pointer).setPageRank(pagerank);
                    }
                }
            }

            for(List<DocInfoEssential> list:bigList){
                for(DocInfoEssential ob:list){
                    long block=ob.getOffset()/_MAX_NUMBER_OF_BYTES_PER_BLOCK_;
                    ob.setProperty(DocInfoEssential.PROPERTY.LENGTH,BigHashMap.get(block).get(ob.getOffset()).getLength());
                    ob.setProperty(DocInfoEssential.PROPERTY.WEIGHT,BigHashMap.get(block).get(ob.getOffset()).getNorm());
                    ob.setProperty(DocInfoEssential.PROPERTY.PAGERANK,BigHashMap.get(block).get(ob.getOffset()).getPageRank());
                    if(((double)ob.getProperty(DocInfoEssential.PROPERTY.WEIGHT))==0.0){
                        System.err.println(ob.getId()+" "+BigHashMap.get(block).get(ob.getOffset()).getNorm());

                    }
//                    System.out.println(ob.getId()+" "+ob.getOffset()+" "+ob.getProperty(DocInfoEssential.PROPERTY.WEIGHT)+" "
//                            +ob.getProperty(DocInfoEssential.PROPERTY.LENGTH)+" "+
//                            ob.getProperty(DocInfoEssential.PROPERTY.PAGERANK));
                }
            }
            return bigList;
        }
    }

    /**
     * Basic method for querying functionality. Given the list of terms in the
     * query, returns a List of Lists of DocInfoFull objects, where each list of
     * DocInfoFull objects holds the DocInfoFull representation of the docs that
     * the corresponding term of the query appears in (i.e., the whole
     * information). Not memory efficient though...
     * <p>
     * Useful when we want to return the title, authors, etc.
     *
     * @param terms
     * @return
     */
    public List<List<DocInfoFull>> getDocInfoFullTerms(List<String> terms) {
        // If indexes are not loaded
        if (!loaded()) {
            return null;
        } else {
            // to implement
            return null;
        }
    }

    /**
     * This is a method that given a list of docs in the essential
     * representation, returns a list with the full description of docs stored
     * in the Documents File. This method is needed when we want to return the
     * full information of a list of documents. Could be useful if we support
     * pagination to the results (i.e. provide the full results of ten
     * documents)
     *
     * @param docs
     * @return
     */
    public List<DocInfoFull> getDocDescription(List<DocInfoEssential> docs) {
        // If indexes are not loaded
        if (!loaded()) {
            return null;
        } else {
            // to implement
            return null;
        }
    }

    /**
     * Method that checks if indexes have been loaded/opened
     *
     * @return
     */
    public static boolean loaded() {
        return __VOCABULARY__ != null && __POSTINGS__ != null
                && __DOCUMENTS__ != null;
    }

    /**
     * Get the path of index as set in themis.config file
     *
     * @return
     */
    public String getIndexDirectory() {
        if (__CONFIG__ != null) {
            return __INDEX_PATH__;
        } else {
            __LOGGER__.error("Index has not been initialized correctly");
            return "";
        }
    }


    String Merged_Voc_File_Name_Creator() {
        return (Merged_Voc_File_Counter++) + "merged" + __VOCABULARY_FILENAME__;
    }


    String Merged_Posting_File_Name_Creator() {
        return (Merged_Post_File_Counter++) + "merged" + __POSTINGS_FILENAME__;
    }
}
