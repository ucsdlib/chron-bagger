package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;
import org.chronopolis.bag.packager.Packager;
import org.chronopolis.bag.packager.PackagerData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Class which writes bags with a given packager
 *
 * Created by shake on 11/16/16.
 */
public class WriteJob implements Callable<WriteResult>, Supplier<WriteResult> {
    private final Logger log = LoggerFactory.getLogger(WriteJob.class);

    private final Bag bag;
    private final boolean validate;
    private final Packager packager;
    private WriteResult result;

    public WriteJob(Bag bag, boolean validate, Packager packager) {
        this.bag = bag;
        this.validate = validate;
        this.packager = packager;
        result = new WriteResult();
    }

    @Override
    public WriteResult call() throws Exception {
        // Is there a better way to get this information? Maybe store it in the bag.
        result.setBag(bag);
        HashFunction hash = bag.getManifest().getDigest().getHashFunction();

        log.info("Starting build for {}", bag.getName());
        PackagerData data = null;
        try {
            data = packager.startBuild(bag.getName());
            TagManifest tagManifest = bag.getTagManifest();
            writePayloadFiles(bag, hash, data);
            writeManifest(bag, hash, tagManifest, data);
            writeTagFiles(bag, hash, tagManifest, data);
            writeTagManifest(bag, hash, tagManifest, data);
        } catch(Exception e) {
            result.setSuccess(false);
            log.error("Error building bag!", e);
        } finally {
            packager.finishBuild(data);
        }

        return result;
    }

    private void writeTagManifest(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) {
        HashCode hashCode;

        // Write the tagmanifest
        log.debug("Writing tagmanifest");
        hashCode = packager.writeManifest(tagManifest, hash, data);
        String receipt = hashCode.toString();
        bag.setReceipt(receipt);
        result.setReceipt(receipt);
        log.debug("HashCode is: {}", receipt);
    }

    private void writeTagFiles(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) {
        HashCode hashCode;

        // Write tag files
        log.info("Writing tag files:");
        for (TagFile tag : bag.getTags().values()) {
            log.debug("{}", tag.getPath());
            hashCode = packager.writeTagFile(tag, hash, data);
            tagManifest.addTagFile(tag.getPath(), hashCode);
            bag.addTag(tag);
            log.debug("HashCode is: {}", hashCode.toString());
        }
    }

    private void writeManifest(Bag bag, HashFunction hash, TagManifest tagManifest, PackagerData data) {
        HashCode hashCode;

        // Write manifest
        log.info("Writing manifest:");
        Manifest manifest = bag.getManifest();
        hashCode = packager.writeManifest(manifest, hash, data);
        tagManifest.addTagFile(manifest.getPath(), hashCode);
        log.debug("HashCode is: {}", hashCode.toString());
    }

    private void writePayloadFiles(Bag bag, HashFunction hash, PackagerData data) {
        HashCode hashCode;

        // Write payload files
        // Validate if wanted
        log.info("Writing payload files");
        if (bag.getFiles().isEmpty()) {
            log.warn("Bag has no payload files, marking as error");
            bag.addBuildError("Bag has no payload files");
        }

        for (PayloadFile payloadFile : bag.getFiles().values()) {
            log.trace(payloadFile.getFile() + ": ");
            hashCode = packager.writePayloadFile(payloadFile, hash, data);
            log.trace(hashCode.toString());

            if (validate && !hashCode.equals(payloadFile.getDigest())) {
                log.error("Digest mismatch for file {}. Expected {}; Found {}",
                    new Object[] {payloadFile, payloadFile.getDigest(), hashCode});
                result.setSuccess(false);
                bag.addError(payloadFile);
            }

            bag.addFile(payloadFile);
        }
    }

    @Override
    public WriteResult get() {
        try {
            return call();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
