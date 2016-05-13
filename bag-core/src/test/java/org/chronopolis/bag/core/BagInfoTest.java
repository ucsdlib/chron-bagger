package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Test for the bag info file
 *
 * Created by shake on 5/12/16.
 */
public class BagInfoTest {

    @Test
    public void getSize() throws Exception {
        BagInfo info = new BagInfo();
        Assert.assertEquals(0, info.getSize());

        info.includeMissingTags(true);
        Assert.assertNotEquals(0, info.getSize());

        // TODO: Could test with some actual tags as well
    }

    @Test
    public void getInputStream() throws Exception {
        BagInfo info = new BagInfo();
        info.withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().toString());
        info.includeMissingTags(true);

        Set<String> tags = ImmutableSet.copyOf(BagInfo.Tag.values()).stream()
                .map(BagInfo.Tag::getName)
                .collect(Collectors.toSet());

        BufferedReader reader = new BufferedReader(new InputStreamReader(info.getInputStream()));
        boolean allMatch = reader.lines()
                .allMatch(s -> {
                    // This will fail on infos with multiple line values but w.e.
                    int i = s.indexOf(":");
                    return tags.contains(s.substring(0, i));
                });

        Assert.assertTrue(allMatch);
    }

    @Test
    public void testClone() {
        BagInfo info = new BagInfo();
        BagInfo clone = info.clone();

        info.withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().toString());
        clone.withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().minusDays(1).toString());

        Assert.assertNotEquals(info, clone);
    }

    @Test
    public void equals() throws Exception {
        BagInfo b1 = new BagInfo().withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().toString());
        BagInfo b2 = new BagInfo().withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().toString());
        BagInfo b3 = new BagInfo().withInfo(BagInfo.Tag.INFO_BAGGING_DATE, LocalDate.now().minusDays(1).toString());

        Assert.assertEquals(b1, b2);
        Assert.assertEquals(b1.hashCode(), b2.hashCode());
        Assert.assertNotEquals(b1, b3);
        Assert.assertNotEquals(b1.hashCode(), b3.hashCode());

    }

}