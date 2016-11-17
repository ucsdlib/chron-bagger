package org.chronopolis.bag;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Created by shake on 5/13/16.
 */
public class SimpleNamingSchemaTest {
    @Test
    public void getName() throws Exception {
        String name = "my-name";
        SimpleNamingSchema schema = new SimpleNamingSchema(name);

        String gen = schema.getName(0);
        Assert.assertEquals(name, gen);

        gen = schema.getName(1);
        Assert.assertNotEquals(name, gen);
        Assert.assertTrue(gen.endsWith("-1"));
    }
}