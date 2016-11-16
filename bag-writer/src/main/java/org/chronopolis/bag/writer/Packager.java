package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Interface describing the actual writing of bags to an output stream
 *
 * Created by shake on 7/30/2015.
 */
public interface Packager {

    /**
     * Start a build using the given bag name
     *
     * @param bagName
     */
    PackagerData startBuild(String bagName);

    /**
     * Finish a build
     *
     */
    void finishBuild(PackagerData data);

    /**
     * Write a tag file to the bag
     *
     * @param tagFile
     * @param function
     * @return the digest of the tag file
     */
    HashCode writeTagFile(TagFile tagFile, HashFunction function, PackagerData data);

    /**
     * Write the manifest file of the bag
     *
     * @param manifest
     * @param function
     * @return the digest of the manifest
     */
    HashCode writeManifest(Manifest manifest, HashFunction function, PackagerData data);

    /**
     * Write a payload file to the data directory of the bag
     *
     * @param payloadFile
     * @param function
     * @return the digest of the payload file
     */
    HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function, PackagerData data);

    default void transfer(InputStream is, OutputStream os) throws IOException {
        ReadableByteChannel inch = Channels.newChannel(is);
        WritableByteChannel wrch = Channels.newChannel(os);

        // 1MB, might want to make this configurable
        ByteBuffer buffer = ByteBuffer.allocateDirect(32768);
        while (inch.read(buffer) != -1) {
            buffer.flip();
            wrch.write(buffer);
            buffer.compact();
        }

        buffer.flip();
        if (buffer.hasRemaining()) {
            wrch.write(buffer);
        }

        buffer.clear();
        inch.close();
        wrch.close();
        is.close();
        os.close();
    }

}
