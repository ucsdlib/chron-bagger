package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 *
 * Created by mike on 8/9/2015.
 */
public class Main {

    public static void main(String[] args) throws IOException, InterruptedException {
        Path base = Paths.get("/home/shake/bags");
        Path out = base.resolve("output");
        Path tag = base.resolve("in/tag-1.txt");
        Path payload = base.resolve("in/manifest-sha256.txt");
        PayloadManifest payloadManifest = PayloadManifest.loadFromStream(Files.newInputStream(payload), base.resolve("in"));
        BagInfo info = new BagInfo()
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "shake@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "ekash@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_NAME, "shake")
                .withInfo(BagInfo.Tag.INFO_CONTACT_PHONE, "phone")
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, "umiacs");

        Writer writer = new SimpleWriter()
                .withPayloadManifest(payloadManifest)
                .withBagInfo(info)
                .withNamingSchema(new SimpleNamingSchema("my-bag"))
                .withPackager(new DirectoryPackager(out))
                .withDigest(Digest.SHA_256)
                .withTagFile(new OnDiskTagFile(tag));


        List<Bag> write = writer.write();
    }

}
