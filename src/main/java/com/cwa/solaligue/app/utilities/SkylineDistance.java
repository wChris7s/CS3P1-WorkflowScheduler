package com.cwa.solaligue.app.utilities;

public class SkylineDistance {

  public double L1 = 0.0;
  public double L2 = 0.0;
  public double P2L = 0.0;
  public double P2Sky = 0.0;

  public String toStringAll() {
    return P2Sky + "\t" + L2 + "\t" + L1 + "\t" + P2L;
  }

  @Override
  public String toString() {
    return P2Sky + "";
  }
}
