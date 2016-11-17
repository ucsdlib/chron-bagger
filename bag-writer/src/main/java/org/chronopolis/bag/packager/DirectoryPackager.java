package org.chronopolis.bag.packager;

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

    public DirectoryPackager(Path base) {
        this.base = base;
    }

    @Override
    public PackagerData startBuild(String name) {
        PackagerData data = new PackagerData();
        Path output = base.resolve(name);

        // TODO: Check if this exists first
        output.toFile().mkdirs();
        data.setName(name);
        data.setWrite(output);
        return data;
    }

    @Override
    public void finishBuild(PackagerData data) {
        // Nothing really to do
    }

    @Override
    // TODO: Optional?
    public HashCode writeTagFile(TagFile tagFile, HashFunction function, PackagerData data) {
        // tagFile.getName() or tagFile.getPath()
        // "sub/dir/name" not really the name
        // "sub/dir/name" is a path
        // allows for things like the dpn-tags easily
        // probably want 1 method for transferring actual bytes/channel
        Path tag = data.getWrite().resolve(tagFile.getPath());
        try {
            return writeFile(tag, function, tagFile.getInputStream());
        } catch (IOException e) {
            log.error("Error writing TagFile {}", tag, e);
        }

        return null;
    }

    @Override
    public HashCode writeManifest(Manifest manifest, HashFunction function, PackagerData data) {
        Path tag = data.getWrite().resolve(manifest.getPath());
        try {
            return writeFile(tag, function, manifest.getInputStream());
        } catch (IOException e) {
            log.error("Error writing Manifest {}", tag, e);
        }

        return null;
    }

    @Override
    public HashCode writePayloadFile(PayloadFile payloadFile, HashFunction function, PackagerData data) {
        // Ensure that all the directories are created first
        Path payload = data.getWrite().resolve(payloadFile.getFile());
        try {
            Files.createDirectories(payload.getParent());
        } catch (IOException e) {
            log.error("Error creating directories for {}", payload, e);
        }

        try {
            return writeFile(payload, function, payloadFile.getInputStream());
        } catch (IOException e) {
            log.error("Error writing PayloadFile {}", payload, e);
        }

        return null;
    }

    protected HashCode writeFile(Path out, HashFunction function, InputStream is) throws IOException {
        OutputStream os = Files.newOutputStream(out, StandardOpenOption.CREATE);
        HashingOutputStream hos = new HashingOutputStream(function, os);
        transfer(is, hos);
        return hos.hash();
    }

}
