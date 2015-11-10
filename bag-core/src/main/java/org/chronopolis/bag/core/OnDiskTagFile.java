package org.chronopolis.bag.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Tag file already held on disk
 *
 * Created by shake on 8/9/2015.
 */
public class OnDiskTagFile implements TagFile {
    private final Logger log = LoggerFactory.getLogger(OnDiskTagFile.class);

    private final Path tag;
    private final Path normalized;

    public OnDiskTagFile(Path tag) {
        // only want the relative location
        this.tag = tag;
        this.normalized = tag.getParent().relativize(tag);
    }

    @Override
    public long getSize() {
        return tag.toFile().length();
    }

    @Override
    public Path getPath() {
        return normalized;
    }

    @Override
    public InputStream getInputStream() {
        try {
            return Files.newInputStream(tag);
        } catch (IOException e) {
            log.error("Error while getting OnDiskTagFile InputStream for {}", tag, e);
            return null;
        }
    }

}
