package org.chronopolis.bag.writer;

import java.io.IOException;
import java.io.InputStream;

/**
 * TODO: Create a bag-test module for all common helpers?
 *
 * Created by shake on 5/16/16.
 */
public class IOExceptionStream extends InputStream {
    @Override
    public int read() throws IOException {
        throw new IOException("this is for testing only");
    }
}
