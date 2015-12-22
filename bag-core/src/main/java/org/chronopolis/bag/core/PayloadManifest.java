package org.chronopolis.bag.core;

import com.google.common.collect.Sets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
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
    private Digest digest;

    private PipedInputStream is;
    private PipedOutputStream os;

    public PayloadManifest() {
        // default
        this.digest = Digest.SHA_256;

        // Use a LinkedHashSet to preserve ordering of the manifest
        this.files = Sets.newLinkedHashSet();
        this.is = new PipedInputStream();
        this.os = new PipedOutputStream();
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

                // TODO: Inject data directory
                PayloadFile payload = new PayloadFile();
                payload.setFile(path);
                payload.setDigest(hash);
                payload.setOrigin(base.resolve(path));
                log.info("Adding payload file {}", path);

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
            String line = file.getDigest().toString() + "  " + file.getFile().toString() + "\n";
            size += line.length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return Paths.get(PAYLOAD_NAME + digest.getBagFormattedName() + SUFFIX);
    }

    @Override
    // TODO: lazyify this
    public InputStream getInputStream() {
        try {
            is.connect(os);
            for (PayloadFile file : files) {
                String line = file.getDigest().toString() + "  " + file.getFile().toString() + "\n";
                os.write(line.getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error while writing payload manifest {}", e);
        }

        return is;
    }

    @Override
    public Digest getDigest() {
        return digest;
    }

    @Override
    public Manifest setDigest(Digest digest) {
        this.digest = digest;
        return this;
    }
}
