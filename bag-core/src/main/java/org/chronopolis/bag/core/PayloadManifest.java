package org.chronopolis.bag.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

/**
 * Flickering in the blue light
 *
 * Created by shake on 7/30/15.
 */
public class PayloadManifest implements Manifest {

    private static final Logger log = LoggerFactory.getLogger(PayloadManifest.class);

    private static final int MAX_SPLIT = 2;

    private Set<PayloadFile> files;
    private final Path path;

    private PipedInputStream is;
    private PipedOutputStream os;

    public PayloadManifest() {
        this.files = new HashSet<>();
        this.is = new PipedInputStream();
        this.os = new PipedOutputStream();
        // TODO: specify the default path or something
        this.path = Paths.get("manifest-sha256.txt");
    }

    public static PayloadManifest loadFromStream(InputStream is, Path base) {
        BufferedReader reader =
                new BufferedReader(new InputStreamReader(is));
        PayloadManifest manifest = new PayloadManifest();

        String line;
        try {
            while ((line = reader.readLine()) != null) {
                String[] split = line.split("\\s+", MAX_SPLIT);
                String hash = split[0];
                String path = split[1];

                PayloadFile payload = new PayloadFile();
                payload.setFile(path);
                payload.setDigest(hash);
                payload.setOrigin(base.resolve(path));

                manifest.addPayloadFile(payload);
            }
        } catch (IOException e) {
            log.error("Error reading manifest", e);
        }

        return manifest;
    }

    public void addPayloadFile(PayloadFile file) {
        log.trace("Adding payload file {}", file.getFile());
        files.add(file);
    }

    public Set<PayloadFile> getFiles() {
        return files;
    }

    @Override
    public long getSize() {
        // TODO: Can memoize this
        long size = 0;
        for (PayloadFile file : files) {
            String line = file.getDigest().toString() + "  " + file.getFile().toString() + "\r\n";
            size += line.length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    // TODO: Fuckin' lazyify this shit
    public InputStream getInputStream() {
        try {
            is.connect(os);
            for (PayloadFile file : files) {
                System.out.println(file.getFile());
                String line = file.getDigest().toString() + "  " + file.getFile().toString() + "\r\n";
                os.write(line.getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error while writing payload manifest {}", e);
        }

        return is;
    }
}
