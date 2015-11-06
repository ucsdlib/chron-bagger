package org.chronopolis.bag.writer;

import com.google.common.collect.ImmutableList;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.Digest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;
import org.chronopolis.bag.core.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Basic implementation of a Writer
 * TODO: Make the bag the unit of work, instead of adding files to it
 *
 * Created by shake on 8/6/2015.
 */
public class SimpleWriter extends Writer {
    private final Logger log = LoggerFactory.getLogger(SimpleWriter.class);

    // Things to help us when writing files
    private Unit maxSize;
    private Digest digest;
    private boolean validate;
    private Packager packager;
    private boolean preserveManifest;
    private NamingSchema namingSchema;

    // Metainformation and the actual files we will be writing
    private BagInfo bagInfo;
    private BagIt bagIt;
    private PayloadManifest manifest;
    private TagManifest tagManifest;
    private List<TagFile> tags;

    // Payload files... TODO: set
    private List<Path> payloadDirectories;
    private List<PayloadFile> payloadFiles;

    public SimpleWriter() {
        super();

        // TODO: Set defaults for all
        digest = Digest.SHA_256;
        validate = false;
        preserveManifest = false;
        this.bagIt = new BagIt();
        this.tags = new ArrayList<>();
        this.payloadFiles = new ArrayList<>();
        this.payloadDirectories = new ArrayList<>();
        this.tagManifest = new TagManifest();
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
    public Writer withMaxSize(Unit maxSize) {
        this.maxSize = maxSize;
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
        this.bagIt = bagIt;
        return this;
    }

    @Override
    public Writer withTagFile(TagFile file) {
        tags.add(file);
        return this;
    }

    @Override
    public Writer withBagInfo(BagInfo bagInfo) {
        this.bagInfo = bagInfo;
        return this;
    }

    @Override
    public Writer withPayloadFile(PayloadFile file) {
        payloadFiles.add(file);
        return this;
    }

    public Writer withPayloadFiles(Set<PayloadFile> files) {
        payloadFiles.addAll(files);
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
        this.manifest = manifest;
        withPayloadFiles(manifest.getFiles());
        return this;
    }

    @Override
    public Writer preserveManifest(boolean preserve) {
        this.preserveManifest = preserve;
        return this;
    }

    @Override
    public List<Bag> write() {
        Bag b = new Bag();
        HashCode hashCode;
        HashFunction hash = digest.getHashFunction();
        log.info("Starting build for {}", namingSchema.getName(0));
        packager.startBuild(namingSchema.getName(0));

        // Write payload files
        // Validate if wanted
        log.trace("Writing payload files");
        for (PayloadFile payloadFile : payloadFiles) {
            log.trace(payloadFile.getFile() + ": ");
            hashCode = packager.writePayloadFile(payloadFile, hash);
            log.trace(hashCode.toString());

            if (validate) {
                if (!hashCode.equals(payloadFile.getDigest())) {
                    log.error("Digest mismatch for file {}. Expected {}; Found {}",
                            new Object[] {payloadFile, payloadFile.getDigest(), hashCode});
                    // TODO: Save error to bag
                }
            }

            b.addFile(payloadFile);
        }

        // Write manifest
        log.trace("Writing manifest:");
        hashCode = packager.writeManifest(manifest, hash);
        tagManifest.addTagFile(manifest.getPath(), hashCode);
        b.setManifest(manifest);
        log.trace("HashCode is: %s\n", hashCode.toString());

        // Write bag-info
        log.trace("Writing bag-info:");
        hashCode = packager.writeTagFile(bagInfo, hash);
        tagManifest.addTagFile(bagInfo.getPath(), hashCode);
        b.addTag(bagInfo);
        log.trace("HashCode is: %s\n", hashCode.toString());

        // Write bagit
        log.trace("Writing bagit:");
        hashCode = packager.writeTagFile(bagIt, hash);
        tagManifest.addTagFile(bagIt.getPath(), hashCode);
        b.addTag(bagIt);
        log.trace("HashCode is: %s\n", hashCode.toString());

        // Write extra tag files
        log.trace("Writing tag files:");
        for (TagFile tag : tags) {
            log.trace("{}", tag.getPath());
            hashCode = packager.writeTagFile(tag, hash);
            tagManifest.addTagFile(tag.getPath(), hashCode);
            b.addTag(tag);
            log.trace("HashCode is: %s", hashCode.toString());
        }

        // Write the tagmanifest
        log.trace("\nWriting tagmanifest:");
        hashCode = packager.writeManifest(tagManifest, hash);
        b.setTagManifest(tagManifest);
        log.trace("HashCode is: %s\n", hashCode.toString());

        packager.finishBuild();
        return ImmutableList.<Bag>of(b);
    }

    @Override
    public List<Future<Bag>> writeAsync() {
        throw new RuntimeException("Not supported");
    }
}
