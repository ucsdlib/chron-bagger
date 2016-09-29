package org.chronopolis.bag.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Tag file already held on disk
 *
 * Created by shake on 8/9/2015.
 */
public class OnDiskTagFile implements TagFile {
    private transient final Logger log = LoggerFactory.getLogger(OnDiskTagFile.class);

    // Store as strings as they're easier to serialize
    private final String tag;
    private final String normalized;

    public OnDiskTagFile(Path tag) {
        // only want the relative location
        this.tag = tag.toString();
        this.normalized = tag.getParent().relativize(tag).toString();
    }

    @Override
    public long getSize() {
        return getTag().toFile().length();
    }

    @Override
    public Path getPath() {
        return Paths.get(normalized);
    }

    private Path getTag() {
        return Paths.get(tag);
    }

    @Override
    public InputStream getInputStream() {
        try {
            return Files.newInputStream(getTag());
        } catch (IOException e) {
            log.error("Error while getting OnDiskTagFile InputStream for {}", tag, e);
            return null;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        OnDiskTagFile that = (OnDiskTagFile) o;

        if (tag != null ? !tag.equals(that.tag) : that.tag != null) return false;
        return normalized != null ? normalized.equals(that.normalized) : that.normalized == null;

    }

    @Override
    public int hashCode() {
        int result = tag != null ? tag.hashCode() : 0;
        result = 31 * result + (normalized != null ? normalized.hashCode() : 0);
        return result;
    }
}
