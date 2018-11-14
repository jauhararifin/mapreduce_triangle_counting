package com.siomay.utils;

public class LongPair implements Comparable<LongPair> {

    private Long first;
    private Long second;

    public LongPair() {
    }

    public LongPair(Long first, Long second) {
        this.first = first;
        this.second = second;
    }

    public Long getFirst() {
        return first;
    }

    public void setFirst(Long first) {
        this.first = first;
    }

    public Long getSecond() {
        return second;
    }

    public void setSecond(Long second) {
        this.second = second;
    }

    public int compareTo(LongPair longPair) {
        if (getFirst().equals(longPair.getFirst())) {
            return getSecond().compareTo(longPair.getSecond());
        }
        return getFirst().compareTo(longPair.getFirst());
    }

    @Override
    public String toString() {
        return "LongPair{" +
                "first=" + first +
                ", second=" + second +
                '}';
    }
}
