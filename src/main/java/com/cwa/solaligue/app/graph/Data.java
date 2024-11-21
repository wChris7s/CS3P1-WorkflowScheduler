package com.cwa.solaligue.app.graph;

import lombok.Getter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

@Getter
public class Data {
  private final long sizeB;
  private final List<String> names;

  public Data(String name, long sizeB) {
    this.sizeB = sizeB;
    this.names = new ArrayList<>();
    names.add(name);
  }

  public Data(Collection<String> names, long sizeB) {
    this.sizeB = sizeB;
    this.names = new ArrayList<>();
    this.names.addAll(names);
  }
}