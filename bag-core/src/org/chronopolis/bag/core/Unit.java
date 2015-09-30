package org.chronopolis.bag.core;

/**
 * Representation of a unit suffix using powers of 1024
 *
 * Created by shake on 7/30/15.
 */
public enum Unit {

    BYTE(0),
    KILOBYTE(1),
    MEGABYTE(2),
    GIGABYTE(3),
    TERABYTE(4);

    private final double size;

    Unit(int exp) {
        this.size = Math.pow(1024, exp);
    }

    public double size() {
        return size;
    }
}
