package org.chronopolis.bag.writer;

/**
 * Schema to name bags based on a given string
 *
 * Created by shake on 7/30/2015.
 */
public class SimpleNamingSchema implements NamingSchema {

    private final String name;

    public SimpleNamingSchema(String name) {
        this.name = name;
    }

    @Override
    public String getName(int bagNumber) {
        StringBuilder nb = new StringBuilder(this.name);
        if (bagNumber > 0) {
            nb.append("-").append(bagNumber);
        }

        return nb.toString();
    }
}
