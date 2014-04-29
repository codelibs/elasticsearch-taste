package org.codelibs.elasticsearch.taste.eval;

public class EvaluationConfig {
    private double trainingPercentage;

    private double evaluationPercentage;

    private float marginForError;

    public double getTrainingPercentage() {
        return trainingPercentage;
    }

    public void setTrainingPercentage(final double trainingPercentage) {
        this.trainingPercentage = trainingPercentage;
    }

    public double getEvaluationPercentage() {
        return evaluationPercentage;
    }

    public void setEvaluationPercentage(final double evaluationPercentage) {
        this.evaluationPercentage = evaluationPercentage;
    }

    public float getMarginForError() {
        return marginForError;
    }

    public void setMarginForError(final float marginForError) {
        this.marginForError = marginForError;
    }

}
