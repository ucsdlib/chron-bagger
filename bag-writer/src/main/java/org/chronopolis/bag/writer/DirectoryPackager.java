package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashingOutputStream;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.TagManifest;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

/**
 * Package bags to a separate directory
 *
 * Created by shake on 8/6/2015.
 */
public class DirectoryPackager implements Packager {

    private final Path base;
    private Path output;

    public DirectoryPackager(Path base) {
        this.base = base;
    }

    @Override
    public void startBuild(String name) {
        output = base.resolve(name);

        // TODO: Check if this exists first
        output.toFile().mkdirs();
    }

    @Override
    public void finishBuild() {

    }

    @Override
    public HashCode writeTagFile(TagFile tagFile, HashFunction function) {
        OutputStream os = null;
        HashingOutputStream hos = null;
        // tagFile.getName() or tagFile.getPath()
        // "sub/dir/name" not really the name
        // "sub/dir/name" is a path
        // allows for things like the dpn-tags easily
        // probably want 1 method for transferring actual bytes/channel
        Path tag = output.resolve(tagFile.getPath());
        try {
            os = Files.newOutputStream(tag, StandardOpenOption.CREATE);
            hos = new HashingOutputStream(function, os);
            transfer(tagFile.getInputStream(), hos);
        } catch (IOException e) {
            System.out.println("fuuuuuuuuuuuck in packager");
        }

        return hos.hash();
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        OutputStream os = null;
        HashingOutputStream hos = null;

        Path tag = output.resolve(manifest.getPath());
        try {
            os = Files.newOutputStream(tag, StandardOpenOption.CREATE);
            hos = new HashingOutputStream(function, os);
            System.out.println(manifest.getInputStream());
            transfer(manifest.getInputStream(), hos);
        } catch (IOException e) {
            System.out.println("fuuuuuuuuuuuck in packager");
        }

        return hos.hash();
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function) {
        // Ensure that all the directories are created first
        Path payload = output.resolve(payloadFile.getFile());
        try {
            Files.createDirectories(payload.getParent());
        } catch (IOException e) {
            System.out.println("fuuuuuuuuuuuck in packager::writePayloadFileMkDirs");
        }

        OutputStream os = null;
        HashingOutputStream hos = null;

        try {
            System.out.println(payload);
            os = Files.newOutputStream(payload, StandardOpenOption.CREATE);
            hos = new HashingOutputStream(function, os);
            transfer(payloadFile.getInputStream(), hos);
        } catch (IOException e) {
            System.out.println("fuuuuuuuuuuuck in packager::writePayloadFile");
        }

        return hos.hash();
    }

    @Override
    public HashCode writeTagManifest(TagManifest tagManifest, HashFunction function) {
        throw new RuntimeException("Not yet implemented");
    }

    // TODO: move up to Packager
    private void transfer(InputStream is, OutputStream os) throws IOException {
        // TODO: Channels... or at least nio lols...
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

        inch.close();
        wrch.close();
        is.close();
        os.close();
    }

}
