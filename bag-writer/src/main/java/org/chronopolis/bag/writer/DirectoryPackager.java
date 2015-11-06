package org.chronopolis.bag.writer;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.HashingOutputStream;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private final Logger log = LoggerFactory.getLogger(DirectoryPackager.class);

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
        HashingOutputStream hos = null;
        // tagFile.getName() or tagFile.getPath()
        // "sub/dir/name" not really the name
        // "sub/dir/name" is a path
        // allows for things like the dpn-tags easily
        // probably want 1 method for transferring actual bytes/channel
        Path tag = output.resolve(tagFile.getPath());
        try {
            return writeFile(tag, function, tagFile.getInputStream());
        } catch (IOException e) {
            log.error("Error writing TagFile {}", tag, e);
        }

        return hos.hash();
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function) {
        HashingOutputStream hos = null;

        Path tag = output.resolve(manifest.getPath());
        try {
            return writeFile(tag, function, manifest.getInputStream());
        } catch (IOException e) {
            log.error("Error writing Manifest {}", tag, e);
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
            log.error("Error creating directories for {}", payload, e);
        }

        HashingOutputStream hos = null;

        try {
            return writeFile(payload, function, payloadFile.getInputStream());
        } catch (IOException e) {
            log.error("Error writing PayloadFile {}", payload, e);
        }

        return hos.hash();
    }

    private HashCode writeFile(Path out, HashFunction function, InputStream is) throws IOException {
        OutputStream os = Files.newOutputStream(out, StandardOpenOption.CREATE);
        HashingOutputStream hos = new HashingOutputStream(function, os);
        transfer(is, hos);
        return hos.hash();
    }

    // TODO: move up to Packager
    private void transfer(InputStream is, OutputStream os) throws IOException {
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

        inch.close();
        wrch.close();
        is.close();
        os.close();
    }

}
