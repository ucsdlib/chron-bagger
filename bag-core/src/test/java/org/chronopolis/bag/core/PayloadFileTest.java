package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 5/13/16.
 */
public class PayloadFileTest {

    private String name = "payload-file";
    private String payload = ClassLoader.getSystemClassLoader()
            .getResource("payload").getPath();
    private ImmutableSet<String> lines = ImmutableSet.of("test-payload-file");

    @Test
    public void setFile() throws Exception {
        Path payloadPath = Paths.get("data", name);
        PayloadFile p = new PayloadFile();
        p.setFile(name);

        Assert.assertEquals(payloadPath, p.getFile());
    }

    @Test
    public void setDigest() throws Exception {
        PayloadFile p = new PayloadFile();
        HashCode code = Hashing.sha256().hashString("test-payload-file", Charset.defaultCharset());
        String s = code.toString();

        p.setDigest(code);
        Assert.assertEquals(s, p.getDigest().toString());

        p.setDigest(s);
        Assert.assertEquals(s, p.getDigest().toString());
    }

    @Test
    public void getSize() throws Exception {
        PayloadFile p = new PayloadFile();
        p.setOrigin(Paths.get(payload, name));
        Assert.assertEquals(18, p.getSize());
    }

    @Test
    public void string() {
        HashCode code = Hashing.sha256().hashString("test-payload-file", Charset.defaultCharset());
        PayloadFile p = new PayloadFile();
        p.setFile(name);
        p.setDigest(code);

        String s = code.toString();
        // Build up the string like we would expect in a manifest - "hash  file\n"
        String asString = s + "  data/" + name + "\n";
        Assert.assertEquals(asString, p.toString());
    }

    @Test
    public void getInputStream() throws Exception {
        PayloadFile p = new PayloadFile();
        p.setOrigin(Paths.get(payload, name));

        BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
        boolean contains = reader.lines()
                .allMatch(lines::contains);
        Assert.assertTrue(contains);
    }

}