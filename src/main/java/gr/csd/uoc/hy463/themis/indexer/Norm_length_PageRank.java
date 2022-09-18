package gr.csd.uoc.hy463.themis.indexer;

public class Norm_length_PageRank {
    double norm;
    int length;
    double PageRank;

    public double getNorm() {
        return norm;
    }

    public void setNorm(double norm) {
        this.norm = norm;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public double getPageRank() {
        return PageRank;
    }

    public void setPageRank(double pageRank) {
        PageRank = pageRank;
    }

    public Norm_length_PageRank(double norm, int length, double pageRank) {
        this.norm = norm;
        this.length = length;
        PageRank = pageRank;
    }

    public Norm_length_PageRank() {
    }
}
