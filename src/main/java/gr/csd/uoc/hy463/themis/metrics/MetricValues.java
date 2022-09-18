package gr.csd.uoc.hy463.themis.metrics;

public class MetricValues {
    double max;
    double min;
    double average;
    double mean;

    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public double getMean() {
        return mean;
    }

    public void setMean(double mean) {
        this.mean = mean;
    }

    public MetricValues(double max, double min, double average, double mean) {
        this.max = max;
        this.min = min;
        this.average = average;
        this.mean = mean;
    }

    public MetricValues() {
    }
}
