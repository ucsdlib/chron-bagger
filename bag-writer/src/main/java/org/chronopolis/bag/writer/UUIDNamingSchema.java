package org.chronopolis.bag.writer;

import java.util.UUID;

/**
 * Schema to generate a UUID for each name
 *
 * Created by shake on 7/30/2015.
 */
public class UUIDNamingSchema implements NamingSchema {
    @Override
    public String getName(int bagNumber) {
        return UUID.randomUUID().toString();
    }
}
