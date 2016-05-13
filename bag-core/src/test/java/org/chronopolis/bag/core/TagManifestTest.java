package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 5/13/16.
 */
public class TagManifestTest {

    private final ImmutableSet<String> tags = ImmutableSet.of("bagit.txt");
    private final String bagitHash = "b1d4cb0d13cb1d9ab509ff9048c4f8d9720bf73ec5856320b1c8ed8c81cd61e1";
    private TagManifest manifest;

    @Before
    public void setup() {
        manifest = new TagManifest();
        manifest.addTagFile(Paths.get("bagit.txt"), HashCode.fromString(bagitHash));
    }

    @Test
    public void getFiles() throws Exception {
        Assert.assertTrue(manifest.getFiles().containsKey(Paths.get("bagit.txt")));
    }

    @Test
    public void getSize() throws Exception {
        // An empty manifest
        TagManifest t = new TagManifest();
        Assert.assertEquals(0, t.getSize());

        // A populated manifest
        String line = bagitHash + "  bagit.txt\n";
        Assert.assertEquals(line.length(), manifest.getSize());
    }

    @Test
    public void getInputStream() throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(manifest.getInputStream()));
        boolean match = reader.lines()
                .map(s -> s.split("\\s+", 2))
                .anyMatch(s -> s.length >= 2 && tags.contains(s[1]));
        Assert.assertTrue(match);
    }

    @Test
    public void setDigest() throws Exception {
        manifest.setDigest(Digest.SHA_256);
        Assert.assertEquals(Digest.SHA_256, manifest.getDigest());
        Assert.assertEquals(Paths.get("tagmanifest-sha256.txt"), manifest.getPath());
    }

}