package com.cwa.solaligue.app.utilities;

import java.util.Random;


public class Zipf {

  private int N;
  private double z;
  private double[] probabilities;
  private Random rand = null;
  private long seed = 1;
  private boolean useBinary = true;
  public long seqComparisons = 0;
  public long binComparisons = 0;
  private SimpleMeasurementMinimize control = null;
  private boolean useControl = true;

  public Zipf(int N, double z) {
    this(N, z, 1, true);
  }

  public Zipf(int N, double z, long seed) {
    this(N, z, seed, true);
  }

  public Zipf(int N, double z, long seed, boolean useBinary) {
    this.N = N;
    this.z = z;
    this.seed = seed;
    this.rand = new Random(seed);
    this.useBinary = useBinary;
    this.probabilities = new double[N];

    double sum = 0;
    for (int i = 1; i <= N; i++) {
      sum += (double) 1 / Math.pow(i, z);
    }
    for (int i = 1; i <= N; i++) {
      probabilities[i - 1] = ((double) 1 / Math.pow(i, z) / sum);
      if (i != 1) {
        probabilities[i - 1] += probabilities[i - 2];
      }
    }
    control = new SimpleMeasurementMinimize(2);
  }

  public int next() {
    if (useControl) {
      switch (control.getNextMethodToCall()) {
        case 0:
          return nextSequential();
        case 1:
          return nextBinarySearch();
      }
    }
    if (useBinary) {
      return nextBinarySearch();
    } else {
      return nextSequential();
    }
  }

  private int nextSequential() {
    double r = rand.nextDouble();
    for (int n = 0; n < N; n++) {
      seqComparisons++;
      if (probabilities[n] > r) {
        control.addFeedback(0, seqComparisons);
        return n;
      }
    }
    control.addFeedback(0, seqComparisons);
    return N - 1;
  }

  private int nextBinarySearch() {
    double r = rand.nextDouble();
    int min = 0;
    int max = N - 1;
    int middle;
    while (max - min > 1) {
      middle = min + ((max - min) / 2);
      binComparisons += 2;
      if (probabilities[middle] > r && probabilities[middle - 1] < r) {
        control.addFeedback(1, binComparisons);
        return middle;
      }
      binComparisons++;
      if (probabilities[middle] > r) {
        max = middle;
      } else {
        min = middle;
      }
    }
    if (probabilities[min] > r) {
      control.addFeedback(1, binComparisons);
      return min;
    }
    control.addFeedback(1, binComparisons);
    return max;
  }
}
