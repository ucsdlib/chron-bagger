package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 5/12/16.
 */
public class OnDiskTagFileTest {

    private final ImmutableSet<String> tags = ImmutableSet.of("Hello", "World");
    private final String name = "test-tag.txt";
    private final String nonexistent = "test-tag.txt";
    private OnDiskTagFile tag;

    @Before
    public void setup() {
        String tags = ClassLoader.getSystemClassLoader()
                                 .getResource("tags").getPath();

        tag = new OnDiskTagFile(Paths.get(tags, name));
    }

    @Test
    public void getSize() throws Exception {
        Assert.assertEquals(26, tag.getSize());
    }

    @Test
    public void getPath() throws Exception {
        Assert.assertEquals(name, tag.getPath().toString());
    }

    @Test
    public void getInputStream() throws Exception {
        InputStream inputStream = tag.getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        boolean matches = reader.lines()
                .map(s -> s.split(":", 2))
                .allMatch(s -> s.length >= 2 && tags.contains(s[0]));

        Assert.assertTrue(matches);
    }

    @Test
    public void getInputStreamNonExistent() {
        OnDiskTagFile odtf = new OnDiskTagFile(Paths.get("some-parent-dir", nonexistent));
        Assert.assertNull(odtf.getInputStream());
    }


    @Test
    public void testSerialize() {
        OnDiskTagFile copy = TagFile.copy(tag);

        Assert.assertFalse(tag == copy);
        Assert.assertEquals(tag, copy);
    }


}