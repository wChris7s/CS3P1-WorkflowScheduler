package com.cwa.solaligue.app.scheduler;

import lombok.Getter;

@Getter
public enum ContainerType {

  A(1.0, (0.085), "A"),
  C(4.08 / 0.92, (0.34), "C"),
  E(7.05 / 0.92, (0.57), "E"),
  G(15.9 / 0.92, (1.34), "G"),
  H(27.25 / 0.92, (2.68), "H");

  private final double containerMemory;

  private final double containerCPU;

  private final double containerDisk;

  private final double containerPrice;

  private final String name;

  ContainerType(double containerCPU, double containerPrice, String name) {
    this.containerCPU = containerCPU;
    this.containerMemory = 1.0;
    this.containerDisk = 1.0;
    this.containerPrice = containerPrice;
    this.name = name;
  }

  public static ContainerType getNextSmaller(ContainerType cType) {
    ContainerType prevCType = ContainerType.getSmallest();
    for (ContainerType nextCT : ContainerType.values()) {
      if (nextCT.equals(cType))
        return prevCType;
      prevCType = nextCT;

    }
    return prevCType;
  }

  public static ContainerType getNextLarger(ContainerType cType) {
    int next = 0;
    ContainerType nextCType = ContainerType.getLargest();
    for (ContainerType nextCT : ContainerType.values()) {
      if (next == 1) {
        nextCType = nextCT;
        return nextCType;
      }

      if (nextCT.equals(cType))
        next = 1;

    }
    return nextCType;
  }

  public static ContainerType getSmallest() {
    return ContainerType.values()[0];
  }

  public static ContainerType getLargest() {
    return ContainerType.values()[ContainerType.values().length - 1];
  }
}
