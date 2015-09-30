package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;

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
    void startBuild(String bagName);

    /**
     * Finish a build
     *
     */
    void finishBuild();

    /**
     * Write a tag file to the bag
     *
     * @param tagFile
     * @param function
     * @return the digest of the tag file
     */
    HashCode writeTagFile(TagFile tagFile, HashFunction function);

    /**
     * Write the manifest file of the bag
     *
     * @param manifest
     * @param function
     * @return the digest of the manifest
     */
    HashCode writeManifest(Manifest manifest, HashFunction function);

    /**
     * Write a payload file to the data directory of the bag
     *
     * @param payloadFile
     * @param function
     * @return the digest of the payload file
     */
    HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function);

    /**
     * Write the tagmanifest file for the bag
     *
     * @param tagManifest
     * @param function
     * @return the digest of the tagmanifest file
     */
    HashCode writeTagManifest(TagManifest tagManifest, HashFunction function);

}
