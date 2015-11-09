package org.chronopolis.bag.core;

/**
 *
 * Created by shake on 7/30/15.
 */
public interface Manifest extends TagFile {

    final String SUFFIX = ".txt";
    final String TAG_NAME = "tagmanifest-";
    final String PAYLOAD_NAME = "manifest-";

    Digest getDigest();
    Manifest setDigest(Digest digest);

}
