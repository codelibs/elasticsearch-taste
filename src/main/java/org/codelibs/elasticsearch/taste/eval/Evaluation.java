package org.codelibs.elasticsearch.taste.eval;

public class Evaluation {

    private double score;

    private int noEstimate = 0;

    private int successful = 0;

    private int failure = 0;

    private long totalProcessingTime = 0;

    private long maxProcessingTime = 0;

    private long averageProcessingTime = 0;

    public double getScore() {
        return score;
    }

    public void setScore(final double score) {
        this.score = score;
    }

    public int getNoEstimate() {
        return noEstimate;
    }

    public void setNoEstimate(final int noEstimate) {
        this.noEstimate = noEstimate;
    }

    public int getSuccessful() {
        return successful;
    }

    public void setSuccessful(final int successful) {
        this.successful = successful;
    }

    public int getFailure() {
        return failure;
    }

    public void setFailure(final int failure) {
        this.failure = failure;
    }

    public long getTotalProcessingTime() {
        return totalProcessingTime;
    }

    public void setTotalProcessingTime(final long totalProcessingTime) {
        this.totalProcessingTime = totalProcessingTime;
    }

    public long getMaxProcessingTime() {
        return maxProcessingTime;
    }

    public void setMaxProcessingTime(final long maxProcessingTime) {
        this.maxProcessingTime = maxProcessingTime;
    }

    public long getAverageProcessingTime() {
        return averageProcessingTime;
    }

    public void setAverageProcessingTime(final long averageProcessingTime) {
        this.averageProcessingTime = averageProcessingTime;
    }

}
