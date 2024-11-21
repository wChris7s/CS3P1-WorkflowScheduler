package com.cwa.solaligue.app.scheduler;

import java.util.ArrayList;


public class Container {
  public long id;
  public String name;
  public ContainerType contType;

  public long startofUse_MS;
  public long startofUseDT_MS;
  public long UsedUpTo_MS;
  public long UsedUpToDT_MS;

  public int lastQuantum;

  public ArrayList<Slot> opsschedule;

  public ArrayList<Slot> freeSlots;

  public Container(long cid, ContainerType ctype) {
    id = cid;
    name = "c" + String.valueOf(cid);
    contType = ctype;

    startofUse_MS = -1;
    startofUseDT_MS = -1;
    UsedUpTo_MS = 0;
    UsedUpToDT_MS = 0;

    lastQuantum = -1;

    opsschedule = new ArrayList<>();
    freeSlots = new ArrayList<>();
  }

  public Container(Container c) {
    id = c.id;
    name = c.name;
    contType = c.contType;

    startofUse_MS = c.startofUse_MS;
    startofUseDT_MS = c.startofUseDT_MS;
    UsedUpTo_MS = c.UsedUpTo_MS;
    UsedUpToDT_MS = c.UsedUpToDT_MS;

    lastQuantum = c.lastQuantum;

    opsschedule = new ArrayList<>();
    for (int i = 0; i < c.opsschedule.size(); ++i) {
      opsschedule.add(new Slot(c.opsschedule.get(i)));
    }

    freeSlots = new ArrayList<>();
    for (int i = 0; i < c.freeSlots.size(); ++i) {
      freeSlots.add(new Slot(c.freeSlots.get(i)));
    }
  }

  public void setUsedUpToDT(long time) {
    UsedUpToDT_MS = Math.max(UsedUpToDT_MS, time);
  }

  public void setUsedUpTo(long time) {
    UsedUpTo_MS = Math.max(UsedUpTo_MS, time);
  }

  public void setStart(long time) {
    if (startofUse_MS == -1) {
      startofUse_MS = time;
    } else {

      startofUse_MS = Math.min(startofUse_MS, time);
    }
  }

  public void setStartDT(long time) {
    if (startofUseDT_MS == -1) {
      startofUseDT_MS = time;
    } else {
      startofUseDT_MS = Math.min(startofUseDT_MS, time);
    }

  }

  public Long getFirstAvailTime_atEnd_MS() {
    if (UsedUpTo_MS == 0) return 0L;
    return UsedUpTo_MS;//+1;
  }
}
