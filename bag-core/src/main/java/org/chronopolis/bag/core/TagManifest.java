package org.chronopolis.bag.core;

import com.google.common.hash.HashCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by shake on 7/30/15.
 */
public class TagManifest implements Manifest {
    private final Logger log = LoggerFactory.getLogger(TagManifest.class);

    private PipedInputStream is;
    private PipedOutputStream os;
    private Map<Path, HashCode> files;
    private final Path path;

    public TagManifest() {
        this.files = new HashMap<>();
        this.is = new PipedInputStream();
        this.os = new PipedOutputStream();
        path = Paths.get("tagmanifest-sha256.txt");
    }

    public void addTagFile(Path file, HashCode hash) {
        files.put(file, hash);
    }

    public Map<Path, HashCode> getFiles() {
        return files;
    }

    @Override
    public long getSize() {
        long size = 0;
        for (Map.Entry<Path, HashCode> entry : files.entrySet()) {
            String line = entry.getValue().toString() + "  " + entry.getKey().toString() + "\n";
            size += line.length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return path;
    }

    // TODO: Fuckin' lazyify this shit
    @Override
    public InputStream getInputStream() {
        try {
            is.connect(os);
            for (Map.Entry<Path, HashCode> entry : files.entrySet()) {
                String line = entry.getValue().toString() + "  " + entry.getKey().toString() + "\n";
                os.write(line.getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error while writing TagManifest InputStream", e);
        }

        return is;
    }

}
