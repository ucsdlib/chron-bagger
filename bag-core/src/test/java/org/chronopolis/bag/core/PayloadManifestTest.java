package org.chronopolis.bag.core;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 5/13/16.
 */
public class PayloadManifestTest {

    private final String expectedEntry = "c72975c151a1239cb7d6aaaa052dabaf13798fa35a8175223fe68af6d0ff426f  data/payload/payload-file\n";
    private final String mpath = ClassLoader.getSystemClassLoader()
            .getResource("manifest").getPath();

    private PayloadManifest loadValidManifest() throws IOException {
        String name = "manifest-sha256.txt";
        return PayloadManifest.loadFromStream(Files.newInputStream(Paths.get(mpath, name)),
                Paths.get(ClassLoader.getSystemClassLoader().getResource("payload").getPath()));
    }

    @Test
    public void getDigest() {
        PayloadManifest pm = new PayloadManifest();
        // default
        Assert.assertEquals(Digest.SHA_256, pm.getDigest());

        pm.setDigest(Digest.MD5);
        // after being set
        Assert.assertEquals(Digest.MD5, pm.getDigest());
    }

    @Test
    public void loadFromStream() throws Exception {
        // From our actual resource
        PayloadManifest manifest = loadValidManifest();

        Assert.assertEquals(1, manifest.getFiles().size());
        Assert.assertTrue(manifest.getFiles().containsKey(Paths.get("data/payload/payload-file")));
        Assert.assertEquals(expectedEntry.length(), manifest.getSize());
    }

    @Test
    public void loadFromStreamException() throws IOException {
        // Test an empty manifest
        PayloadManifest manifest = PayloadManifest.loadFromStream(new IOExceptionStream(),
                Paths.get(ClassLoader.getSystemClassLoader().getResource("payload").getPath()));

        Assert.assertEquals(0, manifest.getFiles().size());
        Assert.assertFalse(manifest.getFiles().containsKey(Paths.get("data/payload/payload-file")));
    }

    @Test
    public void getPath() throws Exception {
        PayloadManifest m = new PayloadManifest();
        Assert.assertEquals(Paths.get("manifest-sha256.txt"), m.getPath());
    }

    @Test
    public void getInputStream() throws Exception {
        PayloadManifest manifest = loadValidManifest();

        BufferedReader reader = new BufferedReader(new InputStreamReader(manifest.getInputStream()));

        // Remove the \n and match against it
        boolean match = reader.lines()
                .allMatch(s -> s.equals(expectedEntry.trim()));

        Assert.assertTrue(match);
    }

    @Test
    public void equals() throws Exception {
        PayloadManifest m1 = loadValidManifest();
        PayloadManifest m2 = loadValidManifest();

        boolean eq = m1.equals(m2);
        boolean hs = m1.hashCode() == m2.hashCode();

        Assert.assertTrue(eq);
        Assert.assertTrue(hs);
    }

}