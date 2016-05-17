package org.chronopolis.bag.core;

import java.io.IOException;
import java.io.InputStream;

/**
 * Helper class to allow us to throw IOExceptions during a read
 *
 * Created by shake on 5/16/16.
 */
public class IOExceptionStream extends InputStream {
    @Override
    public int read() throws IOException {
        throw new IOException("this is for testing only");
    }
}
