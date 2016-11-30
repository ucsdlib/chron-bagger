package org.chronopolis.bag.packager;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.chronopolis.bag.core.Digest;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.writer.IOExceptionStream;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

/**
 * Parent class for Packager testing
 *
 * Created by shake on 11/30/16.
 */
public class PackagerTest {

    protected final HashFunction func = Hashing.sha256();

    protected Path  out = com.google.common.io.Files.createTempDir().toPath(); // Paths.get(System.getProperty("java.io.tmpdir"), "bag-writer-tests");
    protected PayloadFile f;

    @Before
    public void setUp() throws Exception {
        String tags = ClassLoader.getSystemClassLoader()
                                 .getResource("payload").getPath();

        Path payload = Paths.get(tags, "payload-file");
        f = new PayloadFile();
        f.setFile("payload-file");
        f.setOrigin(payload);
        f.setDigest(com.google.common.io.Files.hash(payload.toFile(), func));
    }

    @After
    public void tearDown() throws Exception {
        Files.walkFileTree(out, new FileVisitor<Path>() {
            @Override
            public FileVisitResult preVisitDirectory(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path path, BasicFileAttributes basicFileAttributes) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path path, IOException e) throws IOException {
                return FileVisitResult.TERMINATE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path path, IOException e) throws IOException {
                Files.delete(path);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    class IOManifest implements Manifest {

        @Override
        public Digest getDigest() {
            return Digest.SHA_256;
        }

        @Override
        public Manifest setDigest(Digest digest) {
            return this;
        }

        @Override
        public long getSize() {
            return 42;
        }

        @Override
        public Path getPath() {
            return Paths.get("manifest-ioe.txt");
        }

        @Override
        public InputStream getInputStream() {
            return new IOExceptionStream();
        }
    }

    class IOTagFile implements TagFile {

        @Override
        public long getSize() {
            return 42;
        }

        @Override
        public Path getPath() {
            return Paths.get("ioe");
        }

        @Override
        public InputStream getInputStream() {
            return new IOExceptionStream();
        }
    }

    class IOPayloadFile extends PayloadFile {

        @Override
        public Path getFile() {
            return Paths.get("payload-ioe");
        }

        @Override
        public InputStream getInputStream() {
            return new IOExceptionStream();
        }

    }
}