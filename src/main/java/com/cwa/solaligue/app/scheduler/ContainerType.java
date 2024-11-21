package com.cwa.solaligue.app.scheduler;

public enum ContainerType {

  A(0.92 / 0.92, (0.085), "A"),//m1.small     //2.8
  C(4.08 / 0.92, (0.34), "C"),//m1.large     //1.8
  E(7.05 / 0.92, (0.57), "E"),//m2.xlarge    //2.6
  G(15.9 / 0.92, (1.34), "G"),//m2.2xlarge   //2.6
  H(27.25 / 0.92, (2.68), "H");//m2.4xlarge    //2.6

  public static ContainerType getSmallest() {
    return ContainerType.values()[0];
  }

  public static ContainerType getLargest() {
    return ContainerType.values()[ContainerType.values().length - 1];
  }

  public double container_memory_B = 1.0;
  public double container_CPU = 1.0;
  public double containerDisk_B = 1.0;
  public double container_price = 1.0;
  public String name;


  ContainerType(double container_CPU, double container_price, String name) {
    this.container_CPU = container_CPU;
    this.container_memory_B = 1.0;
    this.containerDisk_B = 1.0;
    this.container_price = container_price;
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

  public static boolean isSmaller(ContainerType ct1, ContainerType ct2) {
    for (ContainerType nextCT : ContainerType.values()) {
      if (nextCT == ct2) {
        return false;
      }
      if (nextCT == ct1)
        return true;
    }
    return false;
  }

  public static boolean isLarger(ContainerType ct1, ContainerType ct2) {
    for (ContainerType nextCT : ContainerType.values()) {
      if (nextCT == ct1) {
        return false;
      }
      if (nextCT == ct2)
        return true;
    }
    return false;
  }
}
