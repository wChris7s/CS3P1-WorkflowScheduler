package com.cwa.solaligue.app.utilities;

import java.io.Serializable;

public class RandomParameters implements Serializable {
  public double z;
  public double operatorType;
  public double[] runTime = null;
  public double[] cpuUtil = null;
  public double[] memory = null;
  public double[] dataout = null;
  public Zipf runTimeDist = null;
  public Zipf cpuUtilDist = null;
  public Zipf memoryDist = null;
  public Zipf dataoutDist = null;

  public RandomParameters(double z, double operatorType,
                          double[] runTime, double[] cpuUtil,
                          double[] memory, double[] dataout) {
    this.z = z;
    this.operatorType = operatorType;

    this.runTime = runTime;
    this.cpuUtil = cpuUtil;
    this.memory = memory;
    this.dataout = dataout;

    this.runTimeDist = new Zipf(this.runTime.length, z);
    this.cpuUtilDist = new Zipf(this.cpuUtil.length, z);
    this.memoryDist = new Zipf(this.memory.length, z);
    this.dataoutDist = new Zipf(this.dataout.length, z);
  }
}
