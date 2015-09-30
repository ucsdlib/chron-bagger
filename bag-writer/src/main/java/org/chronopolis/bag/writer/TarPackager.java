package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashingInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;

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
 * Created by shake on 8/28/15.
 */
public class TarPackager implements Packager {

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
        } catch (IOException e) {
        }

        this.outputStream.setLongFileMode(TarArchiveOutputStream.LONGFILE_GNU);
    }

    @Override
    public void finishBuild() {
        try {
            outputStream.close();
        } catch (IOException e) {
        }
    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function) {
        Path tarPath = getTarPath(tagFile.getPath());
        HashingInputStream his = null;
        TarArchiveEntry entry = new TarArchiveEntry(tarPath.toString());
        try {
            entry.setSize(tagFile.getSize());
            outputStream.putArchiveEntry(entry);
            his = new HashingInputStream(function, tagFile.getInputStream());
            long size = transfer(his, outputStream);
            entry.setSize(size);
            System.out.println("Wrote " + size + " bytes maybe");
            outputStream.closeArchiveEntry();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return his.hash();
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        Path tarPath = getTarPath(manifest.getPath());
        TarArchiveEntry entry = new TarArchiveEntry(tarPath.toString());
        HashingInputStream his = null;
        try {
            entry.setSize(manifest.getSize());
            outputStream.putArchiveEntry(entry);
            his = new HashingInputStream(function, manifest.getInputStream());
            long size = transfer(his, outputStream);
            System.out.println("Wrote " + size + " bytes maybe");
            outputStream.closeArchiveEntry();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return his.hash();
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function) {
        Path tarPath = getTarPath(payloadFile.getFile());
        HashingInputStream his = null;
        TarArchiveEntry entry = new TarArchiveEntry(tarPath.toString());
        try {
            System.out.println("Setting payload size of " + payloadFile.getSize());
            entry.setSize(payloadFile.getSize());
            outputStream.putArchiveEntry(entry);
            his = new HashingInputStream(function, payloadFile.getInputStream());
            transfer(his, outputStream);
            outputStream.closeArchiveEntry();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return his.hash();
    }

    private Path getTarPath(Path relPath) {
        Path root = Paths.get(name + "/");
        Path tarPath = root.resolve(relPath);
        return tarPath;
    }

    private long transfer(InputStream is, OutputStream os) throws IOException {
        long written = 0;
        // TODO: Channels... or at least nio lols...
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
        // wrch.close();
        is.close();
        // os.close();
        return written;
    }

}
