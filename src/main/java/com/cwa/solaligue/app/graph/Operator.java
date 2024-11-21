package com.cwa.solaligue.app.graph;

public class Operator {
  private Long id;
  public String name;
  public ResourcesRequirements resourcesRequirements;
  public String className;
  public double cpuBoundness;
  public Long dagID;

  public Operator(String name, ResourcesRequirements resourcesRequirements) {
    this.name = name;
    this.resourcesRequirements = resourcesRequirements;
    this.id = -1L;
    this.dagID = -1L;
  }

  public long getRunTime_MS() {
    return resourcesRequirements.runtime_MS;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public void setDAGId(Long dagID) {
    this.dagID = dagID;
  }

  public long getId() {
    return id;
  }
}
