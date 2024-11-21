package com.cwa.solaligue.app.utilities;

import com.cwa.solaligue.app.scheduler.Plan;
import com.cwa.solaligue.app.scheduler.SolutionSpace;

import java.util.HashSet;


public class Jaccard {
  public static double computeJaccard(SolutionSpace solutions, SolutionSpace combined) {
    double dist;
    HashSet<Plan> inter = new HashSet<>(combined.results);
    inter.retainAll(solutions.results);
    HashSet<Plan> union = new HashSet<>();
    union.addAll(solutions.results);
    union.addAll(combined.results);
    dist = 1 - ((double) inter.size() / (double) union.size());
    return dist;
  }

}
