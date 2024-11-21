package com.cwa.solaligue.app.utilities;

public class SimpleMeasurementMinimize {

    private long methodMeasurements[];

    public SimpleMeasurementMinimize(int numOfMethods) {
        this.methodMeasurements = new long[numOfMethods];
    }

    public void addFeedback(int method, long measurement) {
        methodMeasurements[method] = measurement;
    }

    public int getNextMethodToCall() {
        int min = 0;
        for(int i = 0; i < methodMeasurements.length; ++i) {
            if (methodMeasurements[i] < methodMeasurements[min]) {
                min = i;
            }
        }
        return min;
    }
}
