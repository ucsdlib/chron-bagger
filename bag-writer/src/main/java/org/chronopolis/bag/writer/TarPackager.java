package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashingInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 *
 * Created by shake on 8/28/15.
 */
public class TarPackager implements Packager {
    private final Logger log = LoggerFactory.getLogger(TarPackager.class);

    private final String TAR = ".tar";

    TarArchiveOutputStream outputStream;
    private final Path base;
    private String name;
    private Path tarball;

    public TarPackager(Path base) {
        this.base = base;
    }


    @Override
    public void startBuild(String bagName) {
        this.name = bagName;
        tarball = base.resolve(bagName + TAR);

        // TODO: Check this
        tarball.getParent().toFile().mkdirs();

        try {
            OutputStream os = new FileOutputStream(tarball.toString());
            this.outputStream = new TarArchiveOutputStream(os);
        }catch (FileNotFoundException e) {
            log.error("Error create TarArchiveOutputStream", e);
        }

        this.outputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    }

    @Override
    public void finishBuild() {
        try {
            outputStream.close();
        } catch (IOException e) {
            log.error("Error closing Tar OutputStream", e);
        }
    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function) {
        Path path = getTarPath(tagFile.getPath());
        return writeFile(function, tagFile.getInputStream(), path.toString(), tagFile.getSize());
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        Path path = getTarPath(manifest.getPath());
        return writeFile(function, manifest.getInputStream(), path.toString(), manifest.getSize());
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function) {
        Path path = getTarPath(payloadFile.getFile());
        return writeFile(function, payloadFile.getInputStream(), path.toString(), payloadFile.getSize());
    }

    private HashCode writeFile(HashFunction function, InputStream is,  String path, long size) {
        HashingInputStream his = null;
        TarArchiveEntry entry = new TarArchiveEntry(path);
        try {
            log.trace("Setting payload size of " + size);
            entry.setSize(size);
            outputStream.putArchiveEntry(entry);
            his = new HashingInputStream(function, is);
            transfer(his, outputStream);
            outputStream.closeArchiveEntry();
        } catch (IOException e) {
            log.error("Error writing TarArchiveEntry {}", path, e);
            return null;
        }

        return his.hash();
    }

    private Path getTarPath(Path relPath) {
        Path root = Paths.get(name + "/");
        return root.resolve(relPath);
    }

    /**
     * Transfer the content of the input stream to the output stream
     * without closing the underlying output stream
     *
     * @param is
     * @param os
     * @return
     * @throws IOException
     */
    @Override
    public void transfer(InputStream is, OutputStream os) throws IOException {
        long written = 0;
        ReadableByteChannel inch = Channels.newChannel(is);
        WritableByteChannel wrch = Channels.newChannel(os);

        ByteBuffer buffer = ByteBuffer.allocateDirect(32768);
        int nr;
        while ((nr = inch.read(buffer)) != -1) {
            buffer.flip();
            wrch.write(buffer);
            buffer.compact();
            written += nr;
        }

        buffer.flip();
        if (buffer.hasRemaining()) {
            wrch.write(buffer);
        }

        inch.close();
        is.close();
    }

}
