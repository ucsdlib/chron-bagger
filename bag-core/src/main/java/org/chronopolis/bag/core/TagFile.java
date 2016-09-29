package org.chronopolis.bag.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.Path;

/**
 * Class so we can treat all Tag files the same
 *
 * Created by shake on 7/29/15.
 */
public interface TagFile extends Serializable {

    long getSize();
    Path getPath();

    // TODO: default method?
    // TODO: Move away from inputstream
    // +getTags()
    InputStream getInputStream();

    /**
     * Returns a copy of the object, or null if the object cannot
     * be serialized.
     *
     * via: http://javatechniques.com/blog/faster-deep-copies-of-java-objects/
     */
    static<T> T copy(T orig) {
        final Logger log = LoggerFactory.getLogger(TagFile.class);
        T obj = null;
        try {
            // Write the object out to a byte array
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            out.writeObject(orig);
            out.flush();
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(
                    new ByteArrayInputStream(bos.toByteArray()));
            Object obj2 = in.readObject();
            obj = (T) obj2;
        }
        catch(IOException | ClassNotFoundException e) {
            log.error("Error copying tag file", e);
        }
        return obj;
    }

}
