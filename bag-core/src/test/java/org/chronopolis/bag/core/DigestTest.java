package org.chronopolis.bag.core;

import com.google.common.hash.HashCode;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.Charset;

/**
 *
 * Created by shake on 5/13/16.
 */
public class DigestTest {

    private Digest m = Digest.MD5;
    private Digest s = Digest.SHA_256;

    @Test
    public void getHashFunction() throws Exception {
        String helloWorldMd5 = "5eb63bbbe01eeed093cb22bb8f5acdc3";

        HashCode hashCode = m.getHashFunction().hashString("hello world", Charset.defaultCharset());
        Assert.assertEquals(helloWorldMd5, hashCode.toString());
    }

    @Test
    public void getBagFormattedName() throws Exception {
        Assert.assertEquals("md5", m.getBagFormattedName());
        Assert.assertEquals("sha256", s.getBagFormattedName());
    }

}