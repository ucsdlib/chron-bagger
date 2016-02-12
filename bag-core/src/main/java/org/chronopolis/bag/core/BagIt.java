package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Representation of the BagIt.txt file
 *
 * Created by shake on 7/29/15.
 */
public class BagIt implements TagFile {

    private final Logger log = LoggerFactory.getLogger(BagIt.class);

    // Do we need this? Not really... I mean... just saying...
    // private static final String BAGIT_PATH = "bagit.txt";

    public static final String CRLF = "\r\n";
    public static final String BAGIT_VERSION = "BagIt-Version: 0.97";
    public static final String TAG_CHARSET = "Tag-File-Character-Encoding: UTF-8";

    private final Path path;
    private PipedInputStream is;
    private PipedOutputStream os;
    private ImmutableSet<String> tags;

    public BagIt() {
        this.path = Paths.get("bagit.txt");
        this.tags = ImmutableSet.of(BAGIT_VERSION, CRLF, TAG_CHARSET);
    }


    @Override
    public long getSize() {
        long size = 0;
        for (String tag : tags) {
            size += tag.length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public InputStream getInputStream() {
        is = new PipedInputStream();
        os = new PipedOutputStream();

        try {
            is.connect(os);
            for (String tag : tags) {
                os.write(tag.getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error writing BagIt InputStream", e);
        }

        return is;
    }
}
