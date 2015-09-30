package org.chronopolis.bag.core;

import com.google.common.hash.HashCode;

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

    private Path file;
    private Path origin;
    private HashCode digest;

    public PayloadFile() {
    }

    public Path getFile() {
        return file;
    }

    public void setFile(Path file) {
        this.file = file;
    }

    public void setFile(String file) {
        this.file = Paths.get(file);
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

    public InputStream getInputStream() {
        try {
            return Files.newInputStream(origin);
        } catch (IOException e) {
            System.out.println("Fuuuuuuuuuuuck in payload file");
            return null;
        }
    }

    public String toString() {
        return file.toString();
    }

    public void setOrigin(Path origin) {
        this.origin = origin;
    }
}
