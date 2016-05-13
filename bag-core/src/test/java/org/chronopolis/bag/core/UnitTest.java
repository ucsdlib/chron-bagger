package org.chronopolis.bag.core;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * Created by shake on 5/13/16.
 */
public class UnitTest {
    @Test
    public void size() throws Exception {
        Unit b = Unit.BYTE;
        Unit kb = Unit.KILOBYTE;
        Unit mb = Unit.MEGABYTE;
        Unit gb = Unit.GIGABYTE;
        Unit tb = Unit.TERABYTE;

        Assert.assertEquals(1, b.size(), 0);
        Assert.assertEquals(1000, kb.size(), 0);
        Assert.assertEquals(1000000, mb.size(), 0);
        Assert.assertEquals(1000000000, gb.size(), 0);
        Assert.assertEquals(1000000000000L, tb.size(), 0);
    }
}