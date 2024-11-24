package com.cwa.solaligue.app.scheduler;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

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
    countTypes = new EnumMap<>(ContainerType.class);
    containersList = new ArrayList<>();
    containers = new HashMap<>();
    contUsed = new HashSet<>();
  }

  public Cluster(Cluster c) {
    contToType = new HashMap<>();
    countTypes = new EnumMap<>(ContainerType.class);
    containersList = new ArrayList<>();
    containers = new HashMap<>();
    nextId = c.nextId;
    contUsed = new HashSet<>();

    for (Container cc : c.containers.values()) {
      Container newCounter = new Container(cc);
      containersList.add(newCounter);
      containers.put(newCounter.id, newCounter);

      contToType.put(newCounter.id, newCounter.contType);
      int ccount = 0;
      if (countTypes.containsKey(newCounter.contType)) {
        ccount = countTypes.get(newCounter.contType);
      }
      countTypes.put(newCounter.contType, ++ccount);
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

  public void update(Long containerId, ContainerType containerType) {
    ContainerType oldContainerType = containers.get(containerId).contType;

    containers.get(containerId).contType = containerType;
    contToType.put(containerId, containerType);

    countTypes.put(oldContainerType, countTypes.get(oldContainerType) - 1);
    if (countTypes.get(oldContainerType) == 0) {
      countTypes.remove(oldContainerType);
    }

    if (countTypes.containsKey(containerType)) {
      countTypes.put(containerType, countTypes.get(containerType) + 1);
    } else {
      countTypes.put(containerType, 1);
    }
  }

  public Optional<Long> getRandomContainerId() {
    if (containersList.isEmpty()) {
      return Optional.empty();
    }
    int randomIndex = ThreadLocalRandom.current().nextInt(containersList.size());
    return Optional.of(containersList.get(randomIndex).id);
  }
}
