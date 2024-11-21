package com.cwa.solaligue.app.graph;

public class ResourcesRequirements {
  public long runtime_MS;
  public int memory_MB;
  public double speedup;

  public ResourcesRequirements(long runtime_MS, int memory_MB, double speedup) {
    this.runtime_MS = runtime_MS;
    this.memory_MB = memory_MB;
    this.speedup = speedup;
  }

  public ResourcesRequirements(long runtime_MS, int memory_MB) {
    this.runtime_MS = runtime_MS;
    this.memory_MB = memory_MB;
    this.speedup = 1;
  }
}
