package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.IntStream;

/**
 *
 * Created by shake on 5/16/16.
 */
public class MultipartWriterTest {

    private MultipartWriter writer;
    private MutableTag mutable;

    @Before
    public void setup() throws IOException {
        // setup a mutable tag file for later
        mutable = new MutableTag();
        mutable.strings.add("setup");

        // start our multipart writer
        writer = new MultipartWriter();
        writer.withNamingSchema(new UUIDNamingSchema())
              .withTagFile(mutable)
              .withMaxSize(36, Unit.BYTE);

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

        writer.withPayloadManifest(manifest);
    }

    @Test
    public void preprocess() throws Exception {
        BagInfo info = writer.b.getInfo();

        writer.preprocess();

        List<Bag> bags = writer.bags;

        Assert.assertEquals(4, bags.size());

        // Things we want to make certain of:
        //   - we have a unique index
        //   - we have a cloned info (== check for memory locality)
        //   - we have cloned tag files (== check for tags)
        bags.forEach(b -> {
            MutableTag ourMutable = (MutableTag) b.getTags().getOrDefault(Paths.get("mutable.txt"), new MutableTag());

            Assert.assertEquals(1, b.getTags().size());
            Assert.assertFalse(info == b.getInfo());
            Assert.assertFalse(mutable == ourMutable);
        });
    }

}