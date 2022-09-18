package gr.csd.uoc.hy463.themis.indexer.indexes;

import java.util.HashMap;

public class BigData{
    //double idf = 0; den to kratao kai to upologizo me to 'log' on the fly gia na glitoso 16 byte ( idf = log(N/df) , opou N o ari8mos ton dimosieuseon )
    public int df = 0;
    public HashMap<String, Double> mymap = new HashMap(); // String = Doc_ID
}
