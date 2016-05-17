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

    private transient final Logger log = LoggerFactory.getLogger(BagIt.class);

    // Do we need this? Not really... I mean... just saying...
    // private static final String BAGIT_PATH = "bagit.txt";

    public static final String CRLF = "\r\n";
    public static final String BAGIT_VERSION = "BagIt-Version: 0.97";
    public static final String TAG_CHARSET = "Tag-File-Character-Encoding: UTF-8";

    private final String path;
    private PipedInputStream is;
    private PipedOutputStream os;
    private ImmutableSet<String> tags;

    public BagIt() {
        // this.path = Paths.get("bagit.txt");
        this.path = "bagit.txt";
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
        return Paths.get(path);
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BagIt bagIt = (BagIt) o;

        if (path != null ? !path.equals(bagIt.path) : bagIt.path != null) return false;
        return tags != null ? tags.equals(bagIt.tags) : bagIt.tags == null;

    }

    @Override
    public int hashCode() {
        int result = path != null ? path.hashCode() : 0;
        result = 31 * result + (tags != null ? tags.hashCode() : 0);
        return result;
    }
}
