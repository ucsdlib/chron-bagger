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
@SuppressWarnings({"FieldCanBeLocal", "WeakerAccess"})
public class TarPackager implements Packager {
    private final Logger log = LoggerFactory.getLogger(TarPackager.class);

    private final String TAR = ".tar";

    private final Path base;

    public TarPackager(Path base) {
        this.base = base;
    }

    @Override
    public PackagerData startBuild(String bagName) {
        PackagerData data = new PackagerData();

        TarArchiveOutputStream outputStream = null;
        Path tarball = base.resolve(bagName + TAR);

        // TODO: Check this
        tarball.getParent().toFile().mkdirs();

        try {
            OutputStream os = new FileOutputStream(tarball.toString());
            outputStream = new TarArchiveOutputStream(os);
            outputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
        }catch (FileNotFoundException e) {
            log.error("Error create TarArchiveOutputStream", e);
            // throw an exception
        }

        data.setName(bagName);
        data.setOs(outputStream);

        return data;
    }

    @Override
    public void finishBuild(PackagerData data) {
        try {
            OutputStream os = data.getOs();
            os.close();
        } catch (IOException e) {
            log.error("Error closing Tar OutputStream", e);
        }
    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function, PackagerData data) {
        Path path = getTarPath(data.getName(), tagFile.getPath());
        return writeFile(function, tagFile.getInputStream(), path.toString(), tagFile.getSize(), data);
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function, PackagerData data) {
        Path path = getTarPath(data.getName(), manifest.getPath());
        return writeFile(function, manifest.getInputStream(), path.toString(), manifest.getSize(), data);
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function, PackagerData data) {
        Path path = getTarPath(data.getName(), payloadFile.getFile());
        return writeFile(function, payloadFile.getInputStream(), path.toString(), payloadFile.getSize(), data);
    }

    private HashCode writeFile(HashFunction function, InputStream is,  String path, long size, PackagerData data) {
        HashingInputStream his = null;
        OutputStream os = data.getOs();
        // Figure this out/clean it up a bit
        if (! (os instanceof TarArchiveOutputStream)) {
            log.error("Cannot package tar archive to non tar stream");
            return HashCode.fromInt(0);
        }

        TarArchiveOutputStream outputStream = (TarArchiveOutputStream) os;
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

    private Path getTarPath(String name, Path relPath) {
        Path root = Paths.get(name + "/");
        return root.resolve(relPath);
    }

    /**
     * Transfer the content of the input stream to the output stream
     * without closing the underlying output stream (by calling wrch.close())
     *
     * @param is
     * @param os
     * @return
     * @throws IOException
     */
    @Override
    public void transfer(InputStream is, OutputStream os) throws IOException {
        ReadableByteChannel inch = Channels.newChannel(is);
        WritableByteChannel wrch = Channels.newChannel(os);

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
        is.close();
    }

}
