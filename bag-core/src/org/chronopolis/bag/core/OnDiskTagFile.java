package org.chronopolis.bag.core;

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

    private final Path tag;
    private final Path normalized;

    public OnDiskTagFile(Path tag) {
        // only want the relative location
        this.tag = tag;
        this.normalized = tag.getParent().relativize(tag);
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
            System.out.println("fuuuuuuuuuuuck in tagfile");
            return null;
        }
    }

}
