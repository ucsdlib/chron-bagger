package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;

/**
 * Created by shake on 8/28/15.
 */
public class TarPackager implements Packager {

    @Override
    public void startBuild(String bagName) {

    }

    @Override
    public void finishBuild() {

    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function) {
        return null;
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        return null;
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function) {
        return null;
    }

    @Override
    public HashCode writeTagManifest(TagManifest tagManifest, HashFunction function) {
        return null;
    }
}
