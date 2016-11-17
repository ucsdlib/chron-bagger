package org.chronopolis.bag;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.chronopolis.bag.packager.TarPackager;
import org.chronopolis.bag.partitioner.Bagger;
import org.chronopolis.bag.partitioner.BaggingResult;
import org.chronopolis.bag.writer.ProtoWriter;
import org.chronopolis.bag.writer.WriteManager;
import org.chronopolis.bag.writer.WriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * TODO: It would probably be better to use a custom comparator for certain tag files instead of fucking with the hash code/equals
 *
 * Created by shake on 8/9/2015.
 */
public class Main {
    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException, InterruptedException {
        Path base = Paths.get("/export/gluster/test-bags");
        Path out = base.resolve("out");
        Path tag = base.resolve("in/tag-1.txt");
        Path payload = base.resolve("in/manifest-sha256.txt");
        PayloadManifest payloadManifest = PayloadManifest.loadFromStream(Files.newInputStream(payload), base.resolve("in"));
        BagInfo info = new BagInfo()
                .includeMissingTags(true)
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "shake@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_EMAIL, "ekash@umiacs.umd.edu")
                .withInfo(BagInfo.Tag.INFO_CONTACT_NAME, "shake")
                .withInfo(BagInfo.Tag.INFO_CONTACT_PHONE, "phone")
                .withInfo(BagInfo.Tag.INFO_SOURCE_ORGANIZATION, "umiacs");

        Bagger baggins = new Bagger()
                .withBagInfo(info)
                .withMaxSize(300, Unit.MEGABYTE)
                .withPayloadManifest(payloadManifest)
                .withNamingSchema(new SimpleNamingSchema("test-bags"));

        ProtoWriter writer = new WriteManager()
                .validate(true)
                .withPackager(new TarPackager(out));


        BaggingResult krafted = baggins.partition();
        log.info("success? {}", krafted.isSuccess());
        log.info("rejects: {}", krafted.getRejected());
        if (krafted.isSuccess()) {
            for (Bag bag : krafted.getBags()) {
                log.info("Partitioned bag {} (valid={}, size={}, files={})", new Object[]{bag.getName(), bag.isValid(), bag.getSize(), bag.getFiles().size()});
            }

            List<CompletableFuture<WriteResult>> write = writer.write(krafted.getBags());
            write.forEach(CompletableFuture::join);
        }
        log.info("Finished");
    }

}
