package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;

import static org.chronopolis.bag.core.BagIt.BAGIT_VERSION;
import static org.chronopolis.bag.core.BagIt.TAG_CHARSET;

/**
 *
 * Created by shake on 5/13/16.
 */
public class BagItTest {

    private Set<String> tags = ImmutableSet.of(BAGIT_VERSION, TAG_CHARSET);
    private final Path bagitPath = Paths.get("bagit.txt");
    private BagIt bagit = new BagIt();

    @Test
    public void getSize() throws Exception {
        Assert.assertEquals(55, bagit.getSize());
    }

    @Test
    public void getPath() throws Exception {
        Assert.assertEquals(bagitPath, bagit.getPath());
    }

    @Test
    public void getInputStream() throws Exception {
        InputStream inputStream = bagit.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        boolean matches = reader.lines()
                .allMatch(s -> tags.contains(s));

        Assert.assertTrue(matches);
    }

    @Test
    public void testSerialize() {
        BagIt bagIt = new BagIt();
        BagIt copy = TagFile.copy(bagIt);

        Assert.assertFalse(bagIt == copy);
        Assert.assertEquals(bagIt, copy);
    }



}