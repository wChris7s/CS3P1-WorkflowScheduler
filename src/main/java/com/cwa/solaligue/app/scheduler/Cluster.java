package com.cwa.solaligue.app.scheduler;

import java.util.*;

public class Cluster {
  public Map<Long, Container> containers;
  public List<Container> containersList;
  public Map<Long, ContainerType> contToType;
  public Map<ContainerType, Integer> countTypes;
  public Set<Long> contUsed;

  private long nextId;

  public Cluster() {
    nextId = 0;
    contToType = new HashMap<>();
    countTypes = new HashMap<>();
    containersList = new ArrayList<>();
    containers = new HashMap<>();
    contUsed = new HashSet<>();
  }

  public Cluster(Cluster c) {
    contToType = new HashMap<>();
    countTypes = new HashMap<>();
    containersList = new ArrayList<>();
    containers = new HashMap<>();
    nextId = c.nextId;
    contUsed = new HashSet<>();

    for (Container cc : c.containers.values()) {
      Container newcont = new Container(cc);
      containersList.add(newcont);
      containers.put(newcont.id, newcont);

      contToType.put(newcont.id, newcont.contType);
      int ccount = 0;
      if (countTypes.containsKey(newcont.contType)) {
        ccount = countTypes.get(newcont.contType);
      }
      countTypes.put(newcont.contType, ++ccount);
    }

    contUsed.addAll(c.contUsed);
  }

  public long addContainer(ContainerType ctype) {
    Container c = new Container(nextId, ctype);
    containersList.add(c);
    containers.put(nextId, c);

    contToType.put(nextId, ctype);
    int ccount = 0;
    if (countTypes.containsKey(ctype)) {
      ccount = countTypes.get(ctype);
    }
    countTypes.put(ctype, ++ccount);

    return nextId++;
  }

  public Container getContainer(Long id) {
    return containers.get(id);
  }

  public void update(Long contid, ContainerType ctype) {
    ContainerType oldtype = containers.get(contid).contType;

    containers.get(contid).contType = ctype;
    contToType.put(contid, ctype);

    countTypes.put(oldtype, countTypes.get(oldtype) - 1);
    if (countTypes.get(oldtype) == 0) {
      countTypes.remove(oldtype);
    }

    if (countTypes.containsKey(ctype)) {
      countTypes.put(ctype, countTypes.get(ctype) + 1);
    } else {
      countTypes.put(ctype, 1);
    }
  }
}
