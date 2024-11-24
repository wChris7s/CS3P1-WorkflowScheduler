package com.cwa.solaligue.app.scheduler;

import com.cwa.solaligue.app.utilities.Pair;
import com.cwa.solaligue.app.utilities.Triple;

import java.util.*;

import static java.lang.Double.MAX_VALUE;
import static java.lang.Double.compare;


public class SolutionSpace implements Iterable<Plan> {

  public ArrayList<Plan> results = null;
  public double optimizationTime_MS;


  public SolutionSpace() {
    results = new ArrayList<>();
  }

  public void add(Plan p) {
    results.add(p);
  }

  public void addAll(Collection<Plan> p) {
    results.addAll(p);
  }

  public void addAll(SolutionSpace sp) {
    results.addAll(sp.results);
  }

  public void setOptimizationTime(long optimizationTime_MS) {
    this.optimizationTime_MS = optimizationTime_MS;
  }

  public boolean isEmpty() {
    return results.isEmpty();
  }

  public int size() {
    return results.size();
  }

  public long getFastestTime() {
    long runtime = Long.MAX_VALUE;
    for (Plan p : results) {
      runtime = Math.min(runtime, p.stats.runtime_MS);
    }
    return runtime;

  }

  public long getMaxRuntime() {
    long runtime = 0;
    for (Plan p : results) {
      runtime = Math.max(runtime, p.stats.runtime_MS);
    }
    return runtime;
  }

  public double getMaxCost() {
    double cost = 0;
    for (Plan p : results) {
      cost = Math.max(cost, p.stats.money);
    }
    return cost;
  }

  public long getMinRuntime() {
    long runtime = Long.MAX_VALUE;
    for (Plan p : results) {
      runtime = Math.min(runtime, p.stats.runtime_MS);
    }
    return runtime;
  }

  public double getMinCost() {
    double cost = Double.MAX_VALUE;
    for (Plan p : results) {
      cost = Math.min(cost, p.stats.money);
    }
    return cost;
  }


  public Plan getMinRuntimePlan() {
    long runtime = Long.MAX_VALUE;
    Plan pp = null;
    for (Plan p : results) {
      if (p.stats.runtime_MS < runtime) {
        runtime = p.stats.runtime_MS;
        pp = p;
      }
    }
    return pp;
  }

  public Plan getSlowest() {
    this.sort(true, false);
    return results.get(results.size() - 1);
  }

  public Plan getKnee() {
    Plan tp = null;
    double t = Double.MAX_VALUE;
    for (Plan p : results) {
      double tt = p.stats.runtime_MS * 0.5 - 0.5 * p.stats.money;
      if (tt < t) {
        tp = p;
        t = tt;
      }
    }
    return tp;
  }

  public Plan getMaxUnfairnessPlan(boolean partialSolution) {
    double unfairness = Double.MIN_VALUE;
    Plan temp = null;
    for (Plan p : results) {
      if (partialSolution) {
        if (p.stats.partialUnfairness > unfairness) {
          unfairness = p.stats.partialUnfairness;
          temp = p;
        }
      } else {
        if (p.stats.unfairness > unfairness) {
          unfairness = p.stats.unfairness;
          temp = p;
        }
      }
    }
    return temp;
  }

  public double getMaxUnfairness(boolean partialSolution) {
    Plan p = getMaxUnfairnessPlan(partialSolution);
    if (partialSolution) {
      return p.stats.partialUnfairness;
    } else {
      return p.stats.unfairness;
    }
  }

  public Plan getMinUnfairnessPlan(boolean partialSolution) {
    double unfairness = Double.MAX_VALUE;
    Plan temp = null;
    for (Plan p : results) {
      if (partialSolution) {
        if (p.stats.partialUnfairness < unfairness) {
          unfairness = p.stats.partialUnfairness;
          temp = p;
        }
      } else {
        if (p.stats.unfairness < unfairness) {
          unfairness = p.stats.unfairness;
          temp = p;
        }
      }
    }
    return temp;
  }

  public double getMinUnfairness(boolean partialSolution) {
    Plan p = getMinUnfairnessPlan(partialSolution);
    if (partialSolution) {
      return p.stats.partialUnfairness;
    } else {
      return p.stats.unfairness;
    }
  }

  public void print() {
    System.out.println(this.toString());
  }

  public void smallPrint() {
    ArrayList<Triple<Long, Double, Double>> mtpairs = new ArrayList<>();
    for (Plan p : results) {
      mtpairs.add(new Triple<>(p.stats.runtime_MS, p.stats.money, p.stats.unfairness));
    }
    Collections.sort(mtpairs, new Comparator<Triple<Long, Double, Double>>() {
      @Override
      public int compare(Triple<Long, Double, Double> o1, Triple<Long, Double, Double> o2) {
        return Long.compare(o1.a, o2.a);
      }
    });
    System.out.println("----------------");
    System.out.println(" Time  ----   Money");
    for (Triple<Long, Double, Double> p : mtpairs) {
      System.out.println(p.a + " -- " + p.b + " --- " + p.c);
    }
    System.out.println("----------------");
  }


  public void clear() {
    results.clear();
    optimizationTime_MS = -1;
  }


  public void sort(boolean skyband) {

    Comparator<Plan> planComparator = (Comparator<Plan>) (p1, p2) -> {//


      if (p1.stats.runtime_MS == p2.stats.runtime_MS) {
        if (Math.abs(p1.stats.money - p2.stats.money) < RuntimeConstants.precisionError) {

          if (Math.abs(p1.stats.partialUnfairness - p2.stats.partialUnfairness) < RuntimeConstants.precisionError)
            return compare(p1.stats.contUtilization, p2.stats.contUtilization);//leave it as it is;
          else if (p1.stats.partialUnfairness > p2.stats.partialUnfairness)
            return 1;
          else
            return -1;
        }
        return compare(p1.stats.money, p2.stats.money);
      } else {
        return Long.compare(p1.stats.runtime_MS, p2.stats.runtime_MS);
      }


    };


    Collections.sort(results, planComparator);


  }


  public void sort(boolean isPareto, boolean multi) {

    Comparator<Plan> ParetoPlanComparator = (Comparator<Plan>) (p1, p2) -> {
      if (p1.stats.runtime_MS == p2.stats.runtime_MS) {
        if (Math.abs(p1.stats.money - p2.stats.money) < RuntimeConstants.precisionError) {
          return Double.compare(p1.stats.contUtilization, p2.stats.contUtilization);

          // TODO: if containers number the same add a criterion e.g fragmentation, #idle slots, utilization etc
        }
        return Double.compare(p1.stats.money, p2.stats.money);
      } else {
        return Long.compare(p1.stats.runtime_MS, p2.stats.runtime_MS);
      }
    };


    Comparator<Plan> PlanComparator = (Comparator<Plan>) (p1, p2) -> {
      if (p1.stats.runtime_MS == p2.stats.runtime_MS) {
        return Double.compare(p1.stats.money, p2.stats.money);
      } else {
        return Long.compare(p1.stats.runtime_MS, p2.stats.runtime_MS);
      }
    };


    Comparator<Plan> MultiParetoPlanComparator = (Comparator<Plan>) (p1, p2) -> {//


      if (p1.stats.runtime_MS == p2.stats.runtime_MS) {
        if (Math.abs(p1.stats.money - p2.stats.money) < RuntimeConstants.precisionError) {

          if (Math.abs(p1.stats.partialUnfairness - p2.stats.partialUnfairness) < RuntimeConstants.precisionError)
            return compare(p1.stats.contUtilization, p2.stats.contUtilization);//leave it as it is;
          else if (p1.stats.partialUnfairness > p2.stats.partialUnfairness)
            return 1;
          else
            return -1;
        }
        return compare(p1.stats.money, p2.stats.money);
      } else {
        return Long.compare(p1.stats.runtime_MS, p2.stats.runtime_MS);
      }


    };


    if (isPareto) {
      if (multi) {
        Collections.sort(results, MultiParetoPlanComparator);

      } else {

        Collections.sort(results, ParetoPlanComparator);
      }
    } else {
      Collections.sort(results, PlanComparator);
    }


  }


  @Override
  public Iterator<Plan> iterator() {
    return results.iterator();
  }

  public double getScoreElastic() {
    double maxTime = 0, minTime = MAX_VALUE;
    double maxMoney = 0, minMoney = MAX_VALUE;

    for (Plan p : results) {
      maxMoney = (long) Math.max(maxMoney, p.stats.money);
      minMoney = (long) Math.min(minMoney, p.stats.money);
      maxTime = Math.max(maxTime, p.stats.runtime_MS);
      minTime = Math.min(minTime, p.stats.runtime_MS);
    }

    double score = ((maxTime - minTime) / maxTime) / ((maxMoney - minMoney) / maxMoney);

    return score;
  }

  public void computeSkyline(boolean isPareto, boolean multi) {

    SolutionSpace skyline = new SolutionSpace();

    this.sort(isPareto, multi); // Sort by time breaking equality by sorting by money

    if (multi) {
      Plan previous = null;

      Plan previousFair = null;

      for (Plan est : results) {

        if (previous == null) {
          skyline.add(est);
          previous = est;
          previousFair = est;
          continue;
        }
        if (previous.stats.runtime_MS == est.stats.runtime_MS) {
          // Already sorted by money

          if (Math.abs(previousFair.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError) //TODO use fairness
            if (previousFair.stats.partialUnfairness > est.stats.partialUnfairness) {//use Double.compare. at moheft as well or add precision error
              skyline.add(est);
              previousFair = est;
            }

          continue;
        }
        if (Math.abs(previous.stats.money - est.stats.money) > RuntimeConstants.precisionError) //TODO ji fix or check
          if (previous.stats.money > est.stats.money) {//use Double.compare. at moheft as well or add precision error
            skyline.add(est);
            previous = est;
            previousFair = est;
            continue;
          }

        if (Math.abs(previousFair.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError) //TODO use fairness
          if (previousFair.stats.partialUnfairness > est.stats.partialUnfairness) {//use Double.compare. at moheft as well or add precision error
            skyline.add(est);
            previousFair = est;
          }


      }
    } else {
      Plan previous = null;
      for (Plan est : results) {
        if (previous == null) {
          skyline.add(est);
          previous = est;
          continue;
        }
        if (previous.stats.runtime_MS == est.stats.runtime_MS) {
          // Already sorted by money
          continue;
        }
        if (Math.abs(previous.stats.money - est.stats.money) > RuntimeConstants.precisionError) //TODO ji fix or check
          if (previous.stats.money > est.stats.money) {//use Double.compare. at moheft as well or add precision error
            skyline.add(est);
            previous = est;
          }
      }

    }
    results.clear();
    results.addAll(skyline.results);

  }

  public ArrayList<Plan> getSkyline(boolean multi, boolean partialSolution) {//todo: change

    SolutionSpace skyline = new SolutionSpace();


    this.sort(true, multi); // Sort by time breaking equality by sorting by money

    if (multi) {//multi-objective includes fairness
      Plan previous = null;

      Plan previousFair = null;

      for (Plan est : results) {

        if (previous == null) {
          skyline.add(est);
          previous = est;
          previousFair = est;
          continue;
        }
        if (previous.stats.runtime_MS == est.stats.runtime_MS) {
          // Already sorted by money

          if (partialSolution) {
            if (Math.abs(previousFair.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError && Math.abs(previous.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError) //TODO use fairness
              if (previousFair.stats.partialUnfairness > est.stats.partialUnfairness && previous.stats.partialUnfairness > est.stats.partialUnfairness) {//use Double.compare. at moheft as well or add precision error
                skyline.add(est);
                previousFair = est;
                //      System.out.println("also added" + est.stats.money+ " " + est.stats.runtime_MS+ " " + est.stats.partialUnfairness );
              }
          } else {
            if (Math.abs(previousFair.stats.unfairness - est.stats.unfairness) > RuntimeConstants.precisionError && Math.abs(previous.stats.unfairness - est.stats.unfairness) > RuntimeConstants.precisionError) //TODO use fairness
              if (previousFair.stats.unfairness > est.stats.unfairness && previous.stats.unfairness > est.stats.unfairness) {//use Double.compare. at moheft as well or add precision error
                skyline.add(est);
                previousFair = est;
                //      System.out.println("also added" + est.stats.money+ " " + est.stats.runtime_MS+ " " + est.stats.partialUnfairness );
              }
          }

          continue;
        }
        if (Math.abs(previous.stats.money - est.stats.money) > RuntimeConstants.precisionError)
          if (previous.stats.money > est.stats.money) {//use Double.compare. at moheft as well or add precision error
            skyline.add(est);

            previous = est;

            if (partialSolution) {
              if (Math.abs(previousFair.stats.partialUnfairness - previous.stats.partialUnfairness) > RuntimeConstants.precisionError)
                if (previousFair.stats.partialUnfairness > previous.stats.partialUnfairness) {//use Double.compare. at moheft as well or add precision error
                  previousFair = est;
                }
            } else {
              if (Math.abs(previousFair.stats.unfairness - previous.stats.unfairness) > RuntimeConstants.precisionError)
                if (previousFair.stats.unfairness > previous.stats.unfairness) {//use Double.compare. at moheft as well or add precision error
                  previousFair = est;
                }
            }

            continue;
          }

        if (partialSolution) {
          if (Math.abs(previousFair.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError && Math.abs(previous.stats.partialUnfairness - est.stats.partialUnfairness) > RuntimeConstants.precisionError) //TODO use fairness
            if (previousFair.stats.partialUnfairness > est.stats.partialUnfairness && previous.stats.partialUnfairness > est.stats.partialUnfairness) {//use Double.compare. at moheft as well or add precision error
              skyline.add(est);
              previousFair = est;
              //     System.out.println("also added" + est.stats.money+ " " + est.stats.runtime_MS+ " " + est.stats.partialUnfairness );
            }
        } else {
          if (Math.abs(previousFair.stats.unfairness - est.stats.unfairness) > RuntimeConstants.precisionError && Math.abs(previous.stats.unfairness - est.stats.unfairness) > RuntimeConstants.precisionError) //TODO use fairness
            if (previousFair.stats.unfairness > est.stats.unfairness && previous.stats.unfairness > est.stats.unfairness) {//use Double.compare. at moheft as well or add precision error
              skyline.add(est);
              previousFair = est;
              //     System.out.println("also added" + est.stats.money+ " " + est.stats.runtime_MS+ " " + est.stats.partialUnfairness );
            }
        }


      }
    }//bi-objective for time-money
    else {
      Plan previous = null;
      for (Plan est : results) {
        if (previous == null) {
          skyline.add(est);
          previous = est;
          continue;
        }
        if (previous.stats.runtime_MS == est.stats.runtime_MS) {
          // Already sorted by money
          continue;
        }
        if (Math.abs(previous.stats.money - est.stats.money) > RuntimeConstants.precisionError) //TODO ji fix or check
          if (previous.stats.money > est.stats.money) {//use Double.compare. at moheft as well or add precision error
            skyline.add(est);
            previous = est;
          }
      }
    }

    return skyline.results;

  }


  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();

    for (Plan p : results) {
      sb.append(p.toString());
    }

    sb.append("/////////////////size: " + results.size()).append("\n");

    return sb.toString();
  }


  public void computeSkyline(boolean pruneEnabled, int k, boolean keepWhole, String method, boolean multi,
                             boolean partialSolution, int constraint_mode, double money_constraint, long time_constraint) {

    ArrayList<Plan> skyline = new ArrayList<>();
    if (constraint_mode == 1) {
      // return the schedule that maximizes fairness given the constraints.
      HashSet<Plan> retset = new HashSet<>();
      keepPlanWithinConstraints(money_constraint, time_constraint);
      if (results.size() > 0) {
        retset.add(getMinUnfairnessPlan(partialSolution));
      }
      this.results.clear();
      this.results.addAll(retset);
      return;
    } else if (constraint_mode == 2) {
      // find all plans that satisfy the constraints and then use pruning.
      HashSet<Plan> retset = new HashSet<>();
//            if(!partialSolution)
      keepPlanWithinConstraints(money_constraint, time_constraint);
    }
    // the rest of the code works on the skyline
    skyline = getSkyline(multi, partialSolution);

    if (!pruneEnabled) {

      System.out.println("pruneEnabled false");
      this.results.clear();
      this.results.addAll(skyline);
      return;
    } else {
      if (keepWhole) {
        k = results.size();
      }
      if (k >= skyline.size()) {
        this.results.clear();
        this.results.addAll(skyline);
        return;
      }
      HashSet<Plan> retset = new HashSet<>();
      if (method.equals("Knee")) {

        Knee(skyline, k, retset, multi, partialSolution);
        this.results.clear();
        this.results.addAll(retset);

      } else {
        System.out.println("method not identified " + method);
      }
    }


  }


  public double getScore(Plan p, long longest, double maxcost) {
    return (0.5 * (p.stats.money / maxcost)) + (0.5 * (p.stats.runtime_MS / longest));
  }

  public double getScoreMin(Plan p, long longest, double maxcost) {
    return Math.abs((0.5 * (p.stats.money / maxcost)) - (0.5 * (p.stats.runtime_MS / longest)));
  }


  public HashSet<Plan> Knee(ArrayList<Plan> donotchange, int k, HashSet<Plan> ret, boolean multi, boolean partialSolution) {//TODO: if plans number > k then keep all plans

    addExtremes(donotchange, ret);

    HashMap<Plan, Double> secondDer = new HashMap<>();
    ArrayList<Plan> knees = getKneess(donotchange, secondDer, multi, partialSolution);


    ArrayList<Pair<Plan, Double>> finalMetric = new ArrayList<>();

    double maxSecondDer = 0.0;
    for (Double der : secondDer.values()) {
      maxSecondDer = Math.max(maxSecondDer, der);
    }
    double maxDist = calculateEuclideanMulti(getMaxCostPlan(), getMinCostPlan(), multi, partialSolution);


    for (int j = 1; j < donotchange.size() - 1; ++j) {
      Plan p = donotchange.get(j);
      if (knees.contains(p)) {
        finalMetric.add(new Pair<Plan, Double>(p, MAX_VALUE));
      } else {
        finalMetric.add(new Pair<Plan, Double>(p, ((0.5 * secondDer.get(p)) / maxSecondDer) * ((0.5 * minDist(p, knees, multi, partialSolution)) / maxDist)));//ignore 0.5...

      }
    }

    Collections.sort(finalMetric, new Comparator<Pair<Plan, Double>>() {
      @Override
      public int compare(Pair<Plan, Double> o1, Pair<Plan, Double> o2) {
        return (int) Double.compare(o2.b, o1.b);
      }
    });

    int i = 0;

    while (ret.size() < k && finalMetric.size() > i + 1) {

      ret.add(finalMetric.get(i).a);
      ++i;
    }

    return ret;
  }

  // returns the second der and the knees
  public ArrayList<Plan> getKneess(ArrayList<Plan> pp, HashMap<Plan, Double> planMetric, boolean multi, boolean partialSolution) {
    //sort by one dimension just to get an orderding
    Collections.sort(pp, new Comparator<Plan>() {
      @Override
      public int compare(Plan o1, Plan o2) {

        if (multi) {
          if (o1.stats.money == o2.stats.money) {
            if (Math.abs(o1.stats.runtime_MS - o2.stats.runtime_MS) < RuntimeConstants.precisionError) {

              if (Math.abs(o1.stats.partialUnfairness - o2.stats.partialUnfairness) < RuntimeConstants.precisionError)
                return Double.compare(o1.stats.contUtilization, o2.stats.contUtilization);//leave it as it is;
              else if (o1.stats.partialUnfairness > o2.stats.partialUnfairness)
                return 1;
              else
                return -1;
            }
            return Long.compare(o1.stats.runtime_MS, o2.stats.runtime_MS);
          } else {
            return Double.compare(o1.stats.money, o2.stats.money);
          }
        } else {
          if (o1.stats.money == o2.stats.money) {
            if (Math.abs(o1.stats.runtime_MS - o2.stats.runtime_MS) < RuntimeConstants.precisionError) {

              return Double.compare(o1.stats.contUtilization, o2.stats.contUtilization);//leave it as it is;

            }
            return Long.compare(o1.stats.runtime_MS, o2.stats.runtime_MS);
          } else {
            return Double.compare(o1.stats.money, o2.stats.money);
          }
        }


      }
    });


    SolutionSpace plans = new SolutionSpace();
    ArrayList<Plan> knees = new ArrayList<>();
    plans.addAll(pp);
    int i = 1;
    double d = 0.0;
    for (; i < plans.size() - 1; ++i) {
      Plan p0 = plans.results.get(i - 1);
      Plan p1 = plans.results.get(i);
      Plan p2 = plans.results.get(i + 1);

      //    System.out.println("next is " + " " + p1.stats.money +  " " + p1.stats.runtime_MS +  " "+ p1.stats.unfairness);

      double secder = 0.0;
      //  if(multi)
      secder = plans.getDerMulti(p0, p1, p2, multi, plans, partialSolution);
      //    else
      //       secder = plans.getDer(p0,p1,p2, multi);
      d += secder;
      planMetric.put(p1, secder);
    }
    i = 1;
    double secder_Avg = d / (plans.results.size() - 2);

    for (Plan p : planMetric.keySet()) {

      if (planMetric.get(p) > secder_Avg) {
        knees.add(p);
      }
    }
    return knees;
  }

  public double minDist(Plan p, ArrayList<Plan> knees, boolean multi, boolean partialSolution) {
    Plan r = null;
    double dist = MAX_VALUE;
    double td;
    for (Plan pp : knees) {
      td = calculateEuclideanMulti(p, pp, multi, partialSolution);
      if (td < dist) {
        dist = td;
        r = pp;
      }
    }
    return dist;
  }

  public void addExtremes(ArrayList<Plan> plans, HashSet<Plan> ret) {
    Plan maxCost = null, slowest = null;
    long slowestTime = 0;
    double maxCostCost = 0.0;

    for (Plan p : plans) {
      if (p.stats.runtime_MS > slowestTime) {
        slowest = p;
        slowestTime = p.stats.runtime_MS;
      }
      if (p.stats.money > maxCostCost) {
        maxCost = p;
        maxCostCost = p.stats.money;
      }
    }
    ret.add(slowest);
    ret.add(maxCost);
  }

  public void retainAllAndKeep(SolutionSpace skylinePlansNew, int pruneSkylineSize) {
    HashSet<Plan> tointe = new HashSet<>();
    tointe.addAll(skylinePlansNew.results);

    ArrayList<Plan> t = new ArrayList<Plan>();
    for (int i = 0; i < results.size() && t.size() < pruneSkylineSize; ++i) {
      if (tointe.contains(results.get(i))) {
        t.add(results.get(i));
      }
    }
    results = t;
  }


  public double calculateEuclideanMulti(Plan a, Plan b, boolean multi, boolean partialSolution) {
    double x = a.stats.runtime_MS - b.stats.runtime_MS;
    double y = a.stats.money - b.stats.money;
    double z = a.stats.partialUnfairness - b.stats.partialUnfairness;
    if (!partialSolution)
      z = a.stats.unfairness - b.stats.unfairness;
    if (!multi)
      z = 0.0;
    return Math.sqrt((x * x) + (y * y) + (z * z));
  }

  public Plan getMaxCostPlan() {
    double cost = 0;
    Plan pl = null;

    for (Plan p : results) {
      if (p.stats.money > cost) {
        cost = p.stats.money;
        pl = p;
      }

    }
    return pl;
  }

  public Plan getMinCostPlan() {
    double cost = MAX_VALUE;
    Plan pl = null;

    for (Plan p : results) {
      if (p.stats.money < cost) {
        cost = p.stats.money;
        pl = p;
      }

    }
    return pl;
  }


  public double getDer(Plan p0, Plan p1, Plan p2, Boolean multi, boolean partialSolution) {
    //sort by money first
    Statistics p0Stats = p0.stats;
    Statistics p1Stats = p1.stats;
    Statistics p2Stats = p2.stats;

    double aR = p1Stats.money - p0Stats.money;
    double bR = p1Stats.runtime_MS - p0Stats.runtime_MS;

    double aL = p2Stats.money - p1Stats.money;
    double bL = p2Stats.runtime_MS - p1Stats.runtime_MS;


    double thetaL = bR / aR;
    double thetaR = bL / aL;
    double theta2P1 = Math.abs(thetaL - thetaR);


    return theta2P1;
  }

  public double normalize(double range, double min, double value) {
    if (range == 0) {
      return 0;
    }
    return (value - min) / range;
  }

  public long normalize(long range, long min, long value) {
    if (range == 0) {
      return 0;
    }
    return (value - min) / range;
  }

  public double getDerMulti(Plan p0, Plan p1, Plan p2, boolean multi, SolutionSpace plans, boolean partialSolution) {//getDerMultiYC_combined
    Statistics p0Stats = p0.stats;
    Statistics p1Stats = p1.stats;
    Statistics p2Stats = p2.stats;

    if (p2.stats.money < p1.stats.money || p1.stats.money < p0.stats.money) {
      System.out.println("VALUES ARE NOT SORTED");
      System.exit(0);
    }

    double costRange = plans.getMaxCost() - plans.getMinCost();
    long timeRange = plans.getMaxRuntime() - plans.getMinRuntime();
    double unfairRange = plans.getMaxUnfairness(partialSolution) - plans.getMinUnfairness(partialSolution);

    double minCost = plans.getMinCost();
    long minTime = plans.getMinRuntime();
    double minUnfair = plans.getMinUnfairness(partialSolution);

    double m01 = normalize(costRange, minCost, p1Stats.money) - normalize(costRange, minCost, p0Stats.money);
    double t01 = normalize(timeRange, minTime, p1Stats.runtime_MS) - normalize(timeRange, minTime, p0Stats.runtime_MS);
    double u01 = normalize(unfairRange, minUnfair, p1Stats.partialUnfairness) - normalize(unfairRange, minUnfair, p0Stats.partialUnfairness);
    if (!partialSolution)
      u01 = normalize(unfairRange, minUnfair, p1Stats.unfairness) - normalize(unfairRange, minUnfair, p0Stats.unfairness);

    double m12 = normalize(costRange, minCost, p2Stats.money) - normalize(costRange, minCost, p1Stats.money);
    double t12 = normalize(timeRange, minTime, p2Stats.runtime_MS) - normalize(timeRange, minTime, p1Stats.runtime_MS);
    double u12 = normalize(unfairRange, minUnfair, p2Stats.partialUnfairness) - normalize(unfairRange, minUnfair, p1Stats.partialUnfairness);
    if (!partialSolution)
      u12 = normalize(unfairRange, minUnfair, p2Stats.unfairness) - normalize(unfairRange, minUnfair, p1Stats.unfairness);


    if (!multi) {
      u01 = 0.0;
      u12 = 0.0;
    }


    double theta01;
    if (Math.abs(m01) < 1e-12) {
      // System.out.println("m01 " + m01);
      theta01 = 0.0;
    } else {
      theta01 = (t01 + u01) / (m01);
    }
    double theta12;
    if (Math.abs(m12) < 1e-12) {
      theta12 = 0.0;
    } else {
      theta12 = (t12 + u12) / (m12);
    }

    return theta12 - theta01;

  }


  public void keepPlanWithinConstraints(double money_constraint, long time_constraint) {
    HashSet<Plan> retset = new HashSet<>();
    for (Plan p : results) {
      if (p.stats.money <= money_constraint && p.stats.runtime_MS <= time_constraint) {
        retset.add(p);
      }
    }
    this.results.clear();
    this.results.addAll(retset);
  }

  public List<List<Plan>> getSortedFronts() {
    List<List<Plan>> fronts = new ArrayList<>();
    List<Plan> currentFront = new ArrayList<>();
    Set<Plan> remainingPlans = new HashSet<>(results);

    while (!remainingPlans.isEmpty()) {
        currentFront.clear();
        Iterator<Plan> iterator = remainingPlans.iterator();

        while (iterator.hasNext()) {
            Plan plan = iterator.next();
            boolean dominated = false;

            for (Plan otherPlan : remainingPlans) {
                if (dominates(otherPlan, plan)) {
                    dominated = true;
                    break;
                }
            }

            if (!dominated) {
                currentFront.add(plan);
                iterator.remove();
            }
        }

        fronts.add(new ArrayList<>(currentFront));
    }

    return fronts;
}

private boolean dominates(Plan p1, Plan p2) {
    boolean betterInAnyObjective = false;

    if (p1.stats.runtime_MS < p2.stats.runtime_MS) {
        betterInAnyObjective = true;
    } else if (p1.stats.runtime_MS > p2.stats.runtime_MS) {
        return false;
    }

    if (p1.stats.money < p2.stats.money) {
        betterInAnyObjective = true;
    } else if (p1.stats.money > p2.stats.money) {
        return false;
    }

    if (p1.stats.unfairness < p2.stats.unfairness) {
        betterInAnyObjective = true;
    } else if (p1.stats.unfairness > p2.stats.unfairness) {
        return false;
    }

    return betterInAnyObjective;
}


}


