package gr.csd.uoc.hy463.themis.indexer;

public class newPostingData {
    int df;
    long pointer;
    int index;

    public int getDf() {
        return df;
    }

    public void setDf(int df) {
        this.df = df;
    }

    public long getPointer() {
        return pointer;
    }

    public void setPointer(long pointer) {
        this.pointer = pointer;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public newPostingData(int df, long pointer, int index) {
        this.df = df;
        this.pointer = pointer;
        this.index = index;
    }

    public newPostingData() {
    }
}
