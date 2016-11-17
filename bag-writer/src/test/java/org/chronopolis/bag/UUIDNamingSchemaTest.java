package org.chronopolis.bag;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Created by shake on 5/13/16.
 */
public class UUIDNamingSchemaTest {

    @Test
    public void getName() {
        UUIDNamingSchema schema = new UUIDNamingSchema();
        String name = schema.getName(0);
        String other = schema.getName(1);

        Assert.assertNotEquals(name, other);
        Assert.assertFalse(other.endsWith("-0"));
    }

}