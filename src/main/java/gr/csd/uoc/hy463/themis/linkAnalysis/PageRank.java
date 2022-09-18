package gr.csd.uoc.hy463.themis.linkAnalysis;

import gr.csd.uoc.hy463.themis.config.Config;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.collections.SemanticScholar.S2JsonEntryReader;
import gr.csd.uoc.hy463.themis.lexicalAnalysis.collections.SemanticScholar.S2TextualEntry;
import gr.csd.uoc.hy463.themis.linkAnalysis.graph.Graph;
import gr.csd.uoc.hy463.themis.linkAnalysis.graph.Node;

import javax.naming.ldap.HasControls;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PageRank {
    static Config __CONFIG__;
    static String __DOCUMENTS_FILENAME__;
    static String __INDEX_PATH__;
    public static ArrayList<String> absolutePaths = new ArrayList<>();


    public static void init() {
        __DOCUMENTS_FILENAME__ = __CONFIG__.getDocumentsFileName();
        __INDEX_PATH__ = __CONFIG__.getIndexPath();
    }

    public static void listFilesForFolder(final File folder) {
        System.out.println("---------start of listing paths---------");
        for (final File fileEntry : folder.listFiles()) {
            if (fileEntry.isDirectory()) {
                System.out.println("is a Direcotry: " + fileEntry.getName());
//                listFilesForFolder(fileEntry);
            } else {
//                System.out.println(fileEntry.getName());
//                String[] str=fileEntry.getName().split("-");
//                if(str[0].equals("s2") && str[1].equals("corpus")){
                absolutePaths.add(fileEntry.getAbsolutePath());
                System.out.println(fileEntry.getName());
//                }

            }
        }
        System.out.println("---------end of listing paths---------");

    }

    public static void main(String[] args) throws IOException {
        Graph graph=new Graph();

        __CONFIG__=new Config();
        init();
        listFilesForFolder(new File(__CONFIG__.getDatasetPath()));
//        RandomAccessFile  __DOCUMENTS__ = new RandomAccessFile(__INDEX_PATH__ + __DOCUMENTS_FILENAME__, "rw");
//        long pointer_to_seek=0;
//        byte[] buffer=new byte[4096];
//        __DOCUMENTS__.read(buffer);
        int cur_Path_counter=0;
        int number_of_documents=0;
        int zero_citations=0;
        int total_number_of_ciations=0;
        for (String path : absolutePaths) {
            System.out.println("current file path= " + path);
            BufferedReader reader = new BufferedReader(new FileReader(path));

            String line = reader.readLine();
            while (line != null) {
                number_of_documents++;
                S2TextualEntry textualEntry;
                textualEntry = S2JsonEntryReader.readTextualEntry(line);
                String docID=textualEntry.getId();

                Node node=new Node(docID);
                graph.addNode(node);

                line=reader.readLine();
            }
        }
        int number_of_citations_notIncluded=0;
        for (String path : absolutePaths) {
            System.out.println("indexing file= " + path);
            BufferedReader reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();

            while (line != null) {
                S2TextualEntry textualEntry;
                textualEntry = S2JsonEntryReader.readTextualEntry(line);
                String docID=textualEntry.getId();
                List<String> citations=textualEntry.getCitations();

                if(citations.size()==0){
                    zero_citations++;
                }
                else{
                    total_number_of_ciations+=citations.size();
                }

                for(String citation:citations){
                    if(graph.getNode(citation)!=null){
                        graph.addEdge(graph.getNode(docID),graph.getNode(citation));
                    }
                    else{
                        number_of_citations_notIncluded++;
                    }
                }
                line=reader.readLine();
            }
        }

//        ---------------Creation_of_Transition_matrix-----------------------
        int size_of_matrix=graph.getNumberOfNodes();
        float[][] matrix=new float[size_of_matrix][size_of_matrix];
        HashMap<Node,Integer> positioninList=new HashMap<>();
        int i=0;

        for(int c=0;c<size_of_matrix;c++){
            for(int k=0;k<size_of_matrix;k++){
                matrix[c][k]=0;
            }
        }

        for(Node node:graph.getAdjacencyList().keySet()){
            positioninList.put(node,i);
            i++;
        }


        for(Node node:graph.getAdjacencyList().keySet()){
            int edges=node.getNumberOfOutEdges();
            float rank=1/edges;
            for(Node small_node: graph.getAdjacencyList().get(node).keySet()){



            }
        }

//        ---------------End_of_Creation_of_Transition_matrix-----------------------
        System.out.println("den thelw na prapsw tpt allo" + cur_Path_counter);
        System.out.println("number of paths: " + cur_Path_counter);
        System.out.println("number of docIDs: " + number_of_documents);
        System.out.println("zero_citations: " + zero_citations);
        System.out.println("average citations per docID: " + (total_number_of_ciations/number_of_documents));
        System.out.println("total_number_of_citations: " + total_number_of_ciations);
        System.out.println("number_of_valid_citations: " + (total_number_of_ciations-number_of_citations_notIncluded));
    }

}
