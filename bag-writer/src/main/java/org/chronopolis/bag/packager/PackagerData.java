package org.chronopolis.bag.packager;

import java.io.OutputStream;
import java.nio.file.Path;

/**
 * Encapsulate a few pieces of information for a packager to use
 *
 * Created by shake on 11/16/16.
 */
public class PackagerData {

    private Path write;
    private String name;
    private OutputStream os;

    public PackagerData() {
    }

    public Path getWrite() {
        return write;
    }

    public PackagerData setWrite(Path write) {
        this.write = write;
        return this;
    }

    public String getName() {
        return name;
    }

    public PackagerData setName(String name) {
        this.name = name;
        return this;
    }

    public OutputStream getOs() {
        return os;
    }

    public PackagerData setOs(OutputStream os) {
        this.os = os;
        return this;
    }
}
