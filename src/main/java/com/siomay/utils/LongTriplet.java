package com.siomay.utils;

public class LongTriplet implements Comparable<LongTriplet> {

    private Long first;
    private Long second;
    private Long third;

    public LongTriplet() {
    }

    public LongTriplet(Long first, Long second, Long third) {
        this.first = first;
        this.second = second;
        this.third = third;
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

    public Long getThird() {
        return third;
    }

    public void setThird(Long third) {
        this.third = third;
    }

    public int compareTo(LongTriplet longPair) {
        if (getFirst() == longPair.getFirst()) {
            if (getSecond() == longPair.getSecond()) {
                return getThird().compareTo(longPair.getThird());
            }
            return getSecond().compareTo(longPair.getSecond());
        }
        return getFirst().compareTo(longPair.getFirst());
    }

    @Override
    public String toString() {
        return "LongTriplet{" +
                "first=" + first +
                ", second=" + second +
                ", third=" + third +
                '}';
    }
}
