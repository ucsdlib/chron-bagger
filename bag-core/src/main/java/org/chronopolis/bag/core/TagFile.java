package org.chronopolis.bag.core;

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
    // +getTags()
    InputStream getInputStream();

    /**
     * Returns a copy of the object, or null if the object cannot
     * be serialized.
     *
     * via: http://javatechniques.com/blog/faster-deep-copies-of-java-objects/
     */
    static<T> T copy(T orig) {
        T obj = null;
        System.out.println(orig.getClass().getName());
        try {
            // Write the object out to a byte array
            System.out.println("Creating streams");
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream(bos);
            System.out.println("writing to streams");
            out.writeObject(orig);
            out.flush();
            out.close();

            // Make an input stream from the byte array and read
            // a copy of the object back in.
            ObjectInputStream in = new ObjectInputStream(
                    new ByteArrayInputStream(bos.toByteArray()));
            System.out.println("reading streams");
            Object obj2 = in.readObject();
            System.out.println(obj2.getClass().getName());
            obj = (T) obj2;
        }
        catch(IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return obj;
    }

}
