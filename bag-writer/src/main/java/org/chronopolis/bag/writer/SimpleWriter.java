package org.chronopolis.bag.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.Digest;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;
import org.chronopolis.bag.core.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * Basic implementation of a Writer
 *
 * Created by shake on 8/6/2015.
 */
public class SimpleWriter extends Writer {
    private final Logger log = LoggerFactory.getLogger(SimpleWriter.class);

    // Things to help us when writing files
    protected Digest digest;
    protected boolean validate;
    protected Packager packager;
    protected NamingSchema namingSchema;

    // Metainformation and the actual files we will be writing
    protected Bag b;

    // Payload files... TODO: set
    protected List<Path> payloadDirectories;

    public SimpleWriter() {
        super();

        b = new Bag();
        validate = true;
        digest = Digest.SHA_256;

        // Create defaults for the bag
        // b.setTagManifest(new TagManifest());
    }

    @Override
    public Writer validate(boolean validate) {
        this.validate = validate;
        return this;
    }

    @Override
    public Writer withDigest(Digest digest) {
        this.digest = digest;
        return this;
    }

    @Override
    public Writer withMaxSize(int size, Unit unit) {
        log.warn("SimpleWriter only creates a single bag, ignoring MaxSize setting");
        return this;
    }

    @Override
    public Writer withNamingSchema(NamingSchema namingSchema) {
        this.namingSchema = namingSchema;
        return this;
    }

    @Override
    public Writer withPackager(Packager packager) {
        this.packager = packager;
        return this;
    }

    @Override
    public Writer withBagIt(BagIt bagIt) {
        b.addTag(bagIt);
        return this;
    }

    @Override
    public Writer withTagFile(TagFile file) {
        b.addTag(file);
        return this;
    }

    @Override
    public Writer withBagInfo(BagInfo bagInfo) {
        b.setInfo(bagInfo);
        return this;
    }

    @Override
    public Writer withPayloadFile(PayloadFile file) {
        b.addFile(file);
        return this;
    }

    private Writer withPayloadFiles(Map<Path, PayloadFile> files) {
        log.debug("Adding payload files to bag ({})", files.size());
        b.addFiles(files);
        return this;
    }

    @Override
    public Writer withPayloadDirectory(Path directory) {
        if (directory.toFile().isFile()) {
            log.error("Unable to add file {} as a directory", directory);
            return this;
        }

        payloadDirectories.add(directory);
        return this;
    }

    @Override
    public Writer withPayloadManifest(PayloadManifest manifest) {
        b.setManifest(manifest);
        withPayloadFiles(manifest.getFiles());
        return this;
    }

    @Override
    public Writer preserveManifest(boolean preserve) {
        return this;
    }

    @Override
    public List<Bag> write() {
        b.prepareForWrite();
        String name = namingSchema.getName(0);
        b.setName(name);
        writeBag(b);
        return ImmutableList.of(b);
    }

    protected void writeBag(Bag bag) {
        HashCode hashCode;
        HashFunction hash = digest.getHashFunction();
        log.info("Starting build for {}", bag.getName());
        packager.startBuild(bag.getName());
        TagManifest tagManifest = bag.getTagManifest();

        // Write payload files
        // Validate if wanted
        log.info("Writing payload files");
        for (PayloadFile payloadFile : bag.getFiles().values()) {
            log.trace(payloadFile.getFile() + ": ");
            hashCode = packager.writePayloadFile(payloadFile, hash);
            log.trace(hashCode.toString());

            if (validate) {
                if (!hashCode.equals(payloadFile.getDigest())) {
                    log.error("Digest mismatch for file {}. Expected {}; Found {}",
                            new Object[] {payloadFile, payloadFile.getDigest(), hashCode});
                    bag.addError(payloadFile);
                }
            }

            bag.addFile(payloadFile);
        }

        // Write manifest
        log.info("Writing manifest:");
        Manifest manifest = bag.getManifest();
        hashCode = packager.writeManifest(manifest, hash);
        tagManifest.addTagFile(manifest.getPath(), hashCode);
        log.debug("HashCode is: {}\n", hashCode.toString());

        // Write tag files
        log.info("Writing tag files:");
        for (TagFile tag : bag.getTags().values()) {
            log.debug("{}", tag.getPath());
            hashCode = packager.writeTagFile(tag, hash);
            tagManifest.addTagFile(tag.getPath(), hashCode);
            bag.addTag(tag);
            log.debug("HashCode is: {}", hashCode.toString());
        }

        // Write the tagmanifest
        log.info("Writing tagmanifest:");
        hashCode = packager.writeManifest(tagManifest, hash);
        bag.setReceipt(hashCode.toString());
        log.debug("HashCode is: {}\n", hashCode.toString());

        packager.finishBuild();
    }

    @Override
    public List<Future<Bag>> writeAsync() {
        throw new RuntimeException("Not supported");
    }

}
