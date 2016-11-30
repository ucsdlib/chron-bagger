package org.chronopolis.bag.packager;

import org.apache.commons.compress.archivers.ar.ArArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.chronopolis.bag.core.BagIt;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 *
 * Created by shake on 11/30/16.
 */
public class TarPackagerTest extends PackagerTest {
    private final Logger log = LoggerFactory.getLogger(TarPackagerTest.class);

    @Test
    public void canCloseFailedTar() {
        String build = "tar-failure";
        TarPackager packager = new TarPackager(out);
        PackagerData data = packager.startBuild(build);

        // Write a little bit to the OutputStream
        BagIt bagIt = new BagIt();
        IOTagFile io = new IOTagFile();
        try {
            packager.writeTagFile(bagIt, func, data);
            packager.writeTagFile(io, func, data);
        } catch (IOException e) {
            log.info("Caught expected exception, attempting to close stream");
        }

        // This will throw an exception
        packager.finishBuild(data);

        boolean exception = false;
        try {
            TarArchiveOutputStream taos = (TarArchiveOutputStream) data.getOs();
            taos.putArchiveEntry(new ArArchiveEntry("test-archive", 100));
        } catch (IOException e) {
            exception = true;
        }

        Assert.assertTrue("Could not write to stream", exception);
    }

}