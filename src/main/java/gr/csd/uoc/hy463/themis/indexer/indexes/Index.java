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
package gr.csd.uoc.hy463.themis.indexer.indexes;

import gr.csd.uoc.hy463.themis.config.Config;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class holds all information related to a specific (partial or not) index
 * in memory. It also knows how to store this information to files
 *
 * @author Panagiotis Papadakos (papadako@ics.forth.gr)
 */
public class Index {

    // Partial indexes have an id > 0 and corresponding idx files are stored in
    // INDEX_PATH/id while for a full index, idx files are stored in INDEX_PATH
    // e.g., the first partial index files are saved to INDEX_PATH/1/
    private int id = 0; // the id of the index that is used for partial indexes

    private static final Logger __LOGGER__ = LogManager.getLogger(Index.class);
    private Config __CONFIG__;  // configuration options

    // The path of index
    private String __INDEX_PATH__ = null;
    // Filenames of indexes
    private String __VOCABULARY_FILENAME__ = null;
    private String __POSTINGS_FILENAME__ = null;
    public String __DOCUMENTS_FILENAME__ = null;
    public Queue<String> Voc_Path_queue = new LinkedList<>();
    public Queue<String> Pos_Path_queue = new LinkedList<>();
    int cterms=0;
    int cdf=0;
    int cdfinside=0;

    // We also need to store any information about the vocabulary,
    // posting and document file in memory
    // For example a TreeMap holds entries sorted which helps with storing the
    // vocabulary file

    private HashMap<String, Integer> __VOCABULARY__ = null;
    public  HashMap<String, BigData> BigWordsMap = new HashMap<>();
    public  HashMap<String,Long> PointersToDocFile= new HashMap<>();
    // We have to hold also other appropriate data structures for postings / documents
    public Index(Config config) {
        __CONFIG__ = config;
        init();
    }

    /**
     * Initialize things
     */
    private void init() {

        __VOCABULARY_FILENAME__ = __CONFIG__.getVocabularyFileName();
        __POSTINGS_FILENAME__ = __CONFIG__.getPostingsFileName();
        __DOCUMENTS_FILENAME__ = __CONFIG__.getDocumentsFileName();
        __INDEX_PATH__ = __CONFIG__.getIndexPath();
        System.out.println("index path "+__INDEX_PATH__);
    }

    /**
     * This method is responsible for dumping all information held by this index
     * to the filesystem in the directory INDEX_PATH/id. If id = 0 then it dumps
     * every idx files to the INDEX_PATH
     *
     * Specifically, it creates:
     *
     * =========================================================================
     * 1) VOCABULARY FILE => vocabulary.idx (Normal Sequential file)
     *
     * This is a normal sequential file where we write in lexicographic order
     * the following entries separated by space: | TERM (a term of the
     * vocabulary) | DF document frequency of this term | POINTER_TO_POSTING
     * (the offset in the posting.idx, this is a long number) |
     *
     * =========================================================================
     * 2) POSTING FILE => posting.idx (Random Access File)
     *
     * For each entry it stores: | DOCUMENT_ID (40 ASCII chars => 40 bytes) | TF
     * (int => 4 bytes) | POINTER_TO_DOCUMENT_FILE (long => 4 bytes)
     *
     * =========================================================================
     * 3) DOCUMENTS FILE => documents.idx (Random Access File)
     *
     * For each entry it stores: | DOCUMENT_ID (40 ASCII chars => 40 bytes) |
     * Title (variable bytes / UTF-8) | Author_1,Author_2, ...,Author_k
     * (variable bytes / UTF-8) | AuthorID_1, AuthorID_2, ...,Author_ID_k
     * (variable size /ASCII) | Year (short => 2 bytes)| Journal Name (variable
     * bytes / UTF-8) | The weight (norm) of Document (double => 8 bytes)|
     * Length of Document (int => 4 bytes) | PageRank Score (double => 8 bytes
     * => this will be used in the second phase of the project)
     *
     * ==> IMPORTANT NOTES
     *
     * For strings that have a variable size, just add as an int (4 bytes)
     * prefix storing the size in bytes of the string. Also make sure that you
     * use the correct representation ASCII (1 byte) or UTF-8 (2 bytes). For
     * example the doc id is a hexadecimal hash so there is no need for UTF
     * encoding
     *
     * Authors are separated by a comma
     *
     * Author ids are also separated with a comma
     *
     * The weight of the document will be computed after indexing the whole
     * collection by scanning the whole postings list
     *
     * For now add 0.0 for PageRank score (a team will be responsible for
     * computing it in the second phase of the project)
     *
     *
     * @return
     */

    public boolean dump() throws IOException {
        if (id == 0) {
            // dump to INDEX_PATH
            FileOutputStream  vocFile = new FileOutputStream (__INDEX_PATH__+__VOCABULARY_FILENAME__,true);
            RandomAccessFile postFile= new RandomAccessFile(__INDEX_PATH__+__POSTINGS_FILENAME__,"rw");

            for(String term:BigWordsMap.keySet()){
                HashMap<String,Double> tmp=BigWordsMap.get(term).mymap;
                long postingPointer=postFile.getFilePointer();
                String vocInput=term+"\t"+BigWordsMap.get(term).df+"\t"+postingPointer+"\n";
                vocFile.write(vocInput.getBytes());
                for(String docid:tmp.keySet()){
                    double tf=tmp.get(docid);
                    long p=PointersToDocFile.get(docid);
                    String postInput=docid+"\t"+tf+"\t"+p+"\n";
                    postFile.seek(postFile.length());
                    postFile.writeUTF(postInput);
                }

            }
            vocFile.close();
            postFile.close();


        } else {
            // dump to INDEX_PATH/id
            //tha prepei na ftiaxnoume ta onomata. tha einai ths morfhs __VOCABULARY_FILENAME__1
            //__VOCABULARY_FILENAME__2 klp. tha ta vazoume se mia oura oste kata to merging
            // na vazoume sto telos ths ayta pou paragontai kata to merging klp
            String vocabulary_path = Voc_File_Name_Creator();

            String Path_name_vocabularyFile = __INDEX_PATH__+ vocabulary_path;
            Voc_Path_queue.add(Path_name_vocabularyFile);

            String posting_path = Posting_File_Name_Creator();

            String Path_name_postingFile = __INDEX_PATH__+posting_path ;
            Pos_Path_queue.add(Path_name_postingFile);

            Write_Words_to_VocabularyFile_and_PostFile(Path_name_vocabularyFile, Path_name_postingFile);

        }
        return false;
    }


    private void Write_Words_to_VocabularyFile_and_PostFile(String Path_name_vocabularyFile,
                                                                   String Path_name_postingFile) throws IOException {
        System.out.println("Path_name_vocabularyFile = "+Path_name_vocabularyFile);
        File Vfile = new File(Path_name_vocabularyFile);
        File Pfile = new File(Path_name_postingFile);

        FileWriter  vocFile = new FileWriter (Vfile,true);
        BufferedWriter bw = new BufferedWriter(vocFile);
        RandomAccessFile postFile = new RandomAccessFile(Pfile, "rw");
        long postFile_pointer=0;
        SortedSet<String> sortedSet=new TreeSet<>(BigWordsMap.keySet());
        //for(String term:BigWordsMap.keySet()){
        for(String term:sortedSet){

            HashMap<String,Double> tmp=BigWordsMap.get(term).mymap;
            long postingPointer=postFile.getFilePointer();
            String vocInput=term+"\t"+BigWordsMap.get(term).df+"\t"+postingPointer+"\n";//h grammh oloklhrh opws tha mpei sto vocfile
            bw.append(vocInput);

            ByteBuffer bb= ByteBuffer.allocate(BigWordsMap.get(term).df * 56);

            int offset=0;
            for(String docid:tmp.keySet()){
                double tf=tmp.get(docid);
                long p=PointersToDocFile.get(docid);
//                String postInput=docid+"\t"+tf+"\t"+p+"\n";
                //postFile.seek(postFile_pointer);
//                postFile.writeUTF(postInput);

                bb.put(docid.getBytes());
                //offset+=40;
                bb.putDouble(tf);
                //offset+=8;
                bb.putLong(p);
                //offset+=8;

            }

            postFile.write(bb.array());


            BigWordsMap.get(term).mymap.clear();

        }
        bw.close();
        BigWordsMap.clear();
        PointersToDocFile.clear();
        vocFile.close();
        postFile.close();

    }


    private String Voc_File_Name_Creator() {
        return id+__VOCABULARY_FILENAME__;
    }
    private String Posting_File_Name_Creator() {
        return id+__POSTINGS_FILENAME__;
    }


    public void setID(int id) {
        this.id = id;
    }

    /**
     * Returns if index is partial
     *
     * @return
     */
    public boolean isPartial() {
        return id != 0;
    }

}
