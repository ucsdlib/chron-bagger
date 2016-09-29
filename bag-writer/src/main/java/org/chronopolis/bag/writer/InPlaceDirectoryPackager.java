package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.io.Files;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Packager to support writing bags in place
 *
 * Created by shake on 11/10/15.
 */
public class InPlaceDirectoryPackager extends DirectoryPackager {
    private final Logger log = LoggerFactory.getLogger(InPlaceDirectoryPackager.class);

    private final Path base;

    public InPlaceDirectoryPackager(Path base) {
        super(base);

        this.base = base;
    }

    @Override
    public void startBuild(String name) {
        if (!base.endsWith(name)) {
            log.warn("Top level directory for bag {} does not match", name);
        }
    }

    @Override
    public void finishBuild() {
        // Nothing to do (as of now)
    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function) {
        HashCode hash = null;

        Path tag = base.resolve(tagFile.getPath());
        String errorText = null;

        try {
            if (tag.toFile().exists()) {
                errorText = "Error hashing TagFile {}";
                log.debug("TagFile {} already exists, only hashing", tag);
                hash = Files.hash(tag.toFile(), function);
            } else {
                errorText = "Error writing TagFile {}";
                log.debug("TagFile {} not found, creating", tag);
                hash = writeFile(tag, function, tagFile.getInputStream());
            }
        } catch (IOException e) {
            log.error(errorText, tag, e);
        }

        return hash;
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        return writeTagFile(manifest, function);
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function) {
        // Ensure that all the directories are created first
        Path payload = base.resolve(payloadFile.getFile());
        HashCode hash = null;
        try {
            hash = Files.hash(payload.toFile(), function);
        } catch (IOException e) {
            log.error("Error hashing payload file {}", payload, e);
        }

        return hash;
    }

}
