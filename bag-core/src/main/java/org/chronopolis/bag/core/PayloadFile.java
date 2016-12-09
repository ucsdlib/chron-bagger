package org.chronopolis.bag.core;

import com.google.common.hash.HashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 7/30/15.
 */
public class PayloadFile {

    private static final Logger log = LoggerFactory.getLogger(PayloadFile.class);
    private static final String DATA_DIR = "data/";

    private Path file;
    private Path origin;
    private HashCode digest;

    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        Path cleaned = file;
        // TODO: Do we just want to have everything under data/
        //       even a directory called "data"?
        if (!file.startsWith(DATA_DIR)) {
            cleaned = Paths.get(DATA_DIR, file.toString());
        }

        this.file = cleaned;
    }

    public void setFile(String file) {
        setFile(Paths.get(file));
    }

    public HashCode getDigest() {
        return digest;
    }

    public void setDigest(HashCode digest) {
        this.digest = digest;
    }

    public void setDigest(String digest) {
        this.digest = HashCode.fromString(digest);
    }

    public long getSize() {
        // todo: memoize
        return origin.toFile().length();
    }

    public InputStream getInputStream() {
        try {
            return Files.newInputStream(origin);
        } catch (IOException e) {
            log.error("Error while getting input stream for PayloadFile {}", origin, e);
            return null;
        }
    }

    @Override
    public String toString() {
        return digest.toString() + "  " + file + "\n";
    }

    public void setOrigin(Path origin) {
        this.origin = origin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PayloadFile that = (PayloadFile) o;

        if (file != null ? !file.equals(that.file) : that.file != null) return false;
        if (origin != null ? !origin.equals(that.origin) : that.origin != null) return false;
        return digest != null ? digest.equals(that.digest) : that.digest == null;

    }

    @Override
    public int hashCode() {
        int result = file != null ? file.hashCode() : 0;
        result = 31 * result + (origin != null ? origin.hashCode() : 0);
        result = 31 * result + (digest != null ? digest.hashCode() : 0);
        return result;
    }
}
