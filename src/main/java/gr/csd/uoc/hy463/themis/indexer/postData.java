package gr.csd.uoc.hy463.themis.indexer;

public class postData {
    int voc;
    long pointer;// pointer to postingFile
    int df;

    public int getVoc() {
        return voc;
    }

    public void setVoc(int voc) {
        this.voc = voc;
    }

    public long getPointer() {
        return pointer;
    }

    public void setPointer(long pointer) {
        this.pointer = pointer;
    }

    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public postData(int voc, long pointer, int df) {
        this.voc = voc;
        this.pointer = pointer;
        this.df = df;
    }

    public postData() {
    }
}
