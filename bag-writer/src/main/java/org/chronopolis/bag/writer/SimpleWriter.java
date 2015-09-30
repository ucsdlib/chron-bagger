package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.*;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Basic implementation of a Writer
 *
 * Created by shake on 8/6/2015.
 */
public class SimpleWriter extends Writer {

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
        this.tags = new ArrayList<TagFile>();
        this.payloadFiles = new ArrayList<PayloadFile>();
        this.payloadDirectories = new ArrayList<Path>();
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
        // TODO: If we have either a bag info file or bagit file, handle them separately
        // (for no real reason)

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
        System.out.println(files);
        payloadFiles.addAll(files);
        return this;
    }

    @Override
    public Writer withPayloadDirectory(Path directory) {
        if (directory.toFile().isFile()) {
            // log error
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
        Bag b;
        HashCode hashCode;
        HashFunction hash = digest.getHashFunction();
        System.out.println("Starting build for " + namingSchema.getName(0));
        packager.startBuild(namingSchema.getName(0));

        // Write payload files
        // Validate if wanted
        System.out.println("Writing payload files");
        for (PayloadFile payloadFile : payloadFiles) {
            System.out.print(payloadFile.getFile() + ": ");
            hashCode = packager.writePayloadFile(payloadFile, hash);
            System.out.println(hashCode.toString());

            if (validate) {
                if (!hashCode.equals(payloadFile.getDigest())) {
                    System.out.println("Fuuuuuuuuck mismatch digest");
                }
            }
        }

        // Write manifest
        System.out.print("Writing manifest:");
        hashCode = packager.writeManifest(manifest, hash);
        tagManifest.addTagFile(manifest.getPath(), hashCode);
        System.out.printf("HashCode is: %s\n", hashCode.toString());

        // Write bag-info
        System.out.println("Writing bag-info:");
        hashCode = packager.writeTagFile(bagInfo, hash);
        tagManifest.addTagFile(bagInfo.getPath(), hashCode);
        System.out.printf("HashCode is: %s\n", hashCode.toString());

        // Write bagit
        System.out.println("Writing bagit:");
        hashCode = packager.writeTagFile(bagIt, hash);
        tagManifest.addTagFile(bagIt.getPath(), hashCode);
        System.out.printf("HashCode is: %s\n", hashCode.toString());

        // Write extra tag files
        System.out.println("Writing tag files:");
        for (TagFile tag : tags) {
            System.out.println(tag.getPath());
            hashCode = packager.writeTagFile(tag, hash);
            tagManifest.addTagFile(tag.getPath(), hashCode);
            System.out.printf("HashCode is: %s", hashCode.toString());
        }

        // Write the tagmanifest
        System.out.print("\nWriting tagmanifest:");
        hashCode = packager.writeManifest(tagManifest, hash);
        // tagManifest.addTagFile(manifest.getPath(), hashCode);
        System.out.printf("HashCode is: %s\n", hashCode.toString());

        packager.finishBuild();
        return null;
    }

    @Override
    public List<Future<Bag>> writeAsync() {
        throw new RuntimeException("Not supported");
    }
}
