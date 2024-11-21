package com.cwa.solaligue.app.scheduler;

public  class Slot implements Comparable<Slot>{
    long opId;
    long start_MS;
    long end_MS;

    public Slot(long start_MS,long end_MS){
        this.start_MS = start_MS;
        this.end_MS = end_MS;
        this.opId = -1L;
    }

    public Slot(long opId,long start_MS,long end_MS){
        this.start_MS = start_MS;
        this.end_MS = end_MS;
        this.opId = opId;
    }

    public Slot(Slot s) {
        opId = s.opId;
        start_MS = s.start_MS;
        end_MS = s.end_MS;
    }


    @Override public int compareTo(Slot o) {
        return (int) (this.start_MS - o.start_MS); //TODO test if it works properly
    }
}
