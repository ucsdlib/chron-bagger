package org.chronopolis.bag.partitioner;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.IntStream;

/**
 * Tests for our partitioning functionality
 *
 * Created by shake on 11/18/16.
 */
public class BaggerTest {

    private PayloadManifest manifest() throws IOException {
        String payload = ClassLoader.getSystemClassLoader()
                                   .getResource("payload").getPath();
        Path origin = Paths.get(payload, "payload-file");
        HashCode hash = Files.hash(origin.toFile(), Hashing.sha256());

        // Setup our payload files (7 in total)
        PayloadManifest manifest = new PayloadManifest();
        IntStream.range(1, 8)
                .mapToObj(i -> {
                    PayloadFile f = new PayloadFile();
                    f.setOrigin(origin);
                    f.setFile("payload-file-" + i);
                    f.setDigest(hash);
                    return f;
                })
                .forEach(manifest::addPayloadFile);

        return manifest;
    }

    @Test
    public void partition() throws Exception {
        Bagger bagger = new Bagger()
                .withPayloadManifest(manifest());
        BaggingResult partition = bagger.partition();

        Assert.assertTrue(partition.isSuccess());
        Assert.assertTrue(partition.getRejected().isEmpty());
        Assert.assertEquals(1, partition.getBags().size());
    }

    @Test
    public void partitionReject() throws Exception {
        Bagger bagger = new Bagger()
                .withMaxSize(17, Unit.BYTE)
                .withPayloadManifest(manifest());
        BaggingResult partition = bagger.partition();

        Assert.assertFalse(partition.isSuccess());
        Assert.assertFalse(partition.getRejected().isEmpty());
        Assert.assertEquals(0, partition.getBags().size());
    }

    @Test
    public void partitionMultiple() throws Exception {
        Bagger bagger = new Bagger()
                .withMaxSize(36, Unit.BYTE)
                .withPayloadManifest(manifest());
        BaggingResult partition = bagger.partition();

        Assert.assertTrue(partition.isSuccess());
        Assert.assertTrue(partition.getRejected().isEmpty());
        // 7 files? 8?
        // 4 bags either way
        Assert.assertEquals(4, partition.getBags().size());
    }

}