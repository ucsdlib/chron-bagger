package org.chronopolis.bag.core.support;

import org.chronopolis.bag.core.PayloadFile;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;

/**
 *
 * Created by shake on 2/16/16.
 */
public class PayloadInputStream extends InputStream {

    private ByteBuffer current;
    private Iterator<PayloadFile> it;

    public PayloadInputStream(Iterable<PayloadFile> files) {
        it = files.iterator();
    }

    @Override
    public int read() throws IOException {
        if (current == null || current.position() == current.limit()) {
            PayloadFile f;
            if (it.hasNext()) {
                f = it.next();
            } else {
                return -1;
            }

            current = ByteBuffer.wrap(f.toString().getBytes());
        }

        return current.get();
    }
}
