package org.chronopolis.bag.packager;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.Digest;
import org.chronopolis.bag.core.Manifest;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.writer.IOExceptionStream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;

/**
 *
 * Created by shake on 5/16/16.
 */
public class DirectoryPackagerTest {

    private final HashFunction func = Hashing.sha256();
    private final String bagitHash = "b1d4cb0d13cb1d9ab509ff9048c4f8d9720bf73ec5856320b1c8ed8c81cd61e1";

    private Path out = com.google.common.io.Files.createTempDir().toPath(); // Paths.get(System.getProperty("java.io.tmpdir"), "bag-writer-tests");

    private PayloadFile f;

    @Before
    public void setup() throws IOException {

        String tags = ClassLoader.getSystemClassLoader()
                                 .getResource("payload").getPath();

        Path payload = Paths.get(tags, "payload-file");
        f = new PayloadFile();
        f.setFile("payload-file");
        f.setOrigin(payload);
        f.setDigest(com.google.common.io.Files.hash(payload.toFile(), func));
    }

    @After
    public void teardown() throws IOException {
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

    private DirectoryPackager startPackager() {
        return new DirectoryPackager(out);
    }

    @Test
    public void startBuild() throws Exception {
        String buildName = "packager-test";
        System.out.println("using " + out);
        DirectoryPackager packager = startPackager();
        packager.startBuild(buildName);

        Assert.assertTrue(Files.exists(out.resolve(buildName)));
        Assert.assertTrue(Files.isDirectory(out.resolve(buildName)));
    }

    @Test
    public void writeTagFile() throws Exception {
        String testName = "tag-files";
        Path bag = out.resolve(testName);
        DirectoryPackager packager = startPackager();
        PackagerData packagerData = packager.startBuild(testName);
        BagIt bagIt = new BagIt();
        HashCode hashCode = packager.writeTagFile(bagIt, func, packagerData);

        Assert.assertTrue(Files.exists(bag.resolve("bagit.txt")));
        Assert.assertTrue(Files.isRegularFile(bag.resolve("bagit.txt")));
        Assert.assertEquals(bagitHash, hashCode.toString());
    }

    @Test(expected = IOException.class)
    public void writeTagIO() throws Exception {
        String testName = "tag-files-ioe";

        Path bag = out.resolve(testName);
        DirectoryPackager packager = startPackager();
        PackagerData data = packager.startBuild(testName);
        IOTagFile tag = new IOTagFile();
        HashCode hash = packager.writeTagFile(tag, func, data);

        Assert.assertNull(hash);
        // Assert.assertFalse(Files.exists(bag.resolve(tag.getPath())));
    }

    @Test
    public void writeManifest() throws Exception {
        String testName = "manifest";
        Path bag = out.resolve(testName);

        DirectoryPackager packager = startPackager();
        PackagerData data = packager.startBuild(testName);
        PayloadManifest m = new PayloadManifest();
        m.setDigest(Digest.SHA_256);
        m.addPayloadFile(f);

        HashCode payload = func.hashString(f.toString(), Charset.defaultCharset());
        HashCode pkgHash = packager.writeManifest(m, func, data);
        HashCode fileHash = com.google.common.io.Files.hash(bag.resolve("manifest-sha256.txt").toFile(), func);

        Assert.assertTrue(Files.exists(bag.resolve("manifest-sha256.txt")));
        Assert.assertTrue(Files.isRegularFile(bag.resolve("manifest-sha256.txt")));

        // Test that the returned hashcode and the file on disk equal what we expect
        Assert.assertEquals(payload, pkgHash);
        Assert.assertEquals(payload, fileHash);
    }

    @Test(expected = IOException.class)
    public void writeManifestIO() throws Exception {
        String testName = "manifest-ioe";

        DirectoryPackager packager = startPackager();
        PackagerData data = packager.startBuild(testName);
        Manifest m = new IOManifest();
        HashCode hash = packager.writeManifest(m, func, data);

        Assert.assertNull(hash);
        // Assert.assertFalse(Files.exists(bag.resolve(m.getPath())));
    }

    @Test
    public void writePayloadFile() throws Exception {
        String testName = "payload";
        Path bag = out.resolve(testName);

        DirectoryPackager packager = startPackager();
        PackagerData data = packager.startBuild(testName);
        HashCode hashCode = packager.writePayloadFile(f, func, data);

        String tags = ClassLoader.getSystemClassLoader()
                                 .getResource("payload").getPath();
        HashCode fromResources = com.google.common.io.Files.hash(Paths.get(tags, "payload-file").toFile(), func);

        Path fileInBag = bag.resolve(f.getFile());
        Assert.assertTrue(Files.exists(fileInBag));
        Assert.assertTrue(Files.isRegularFile(fileInBag));

        // Make sure nothing borked while writing, similar to the manifest
        Assert.assertEquals(fromResources, hashCode);
        Assert.assertEquals(f.getDigest(), hashCode);
    }

    @Test(expected = IOException.class)
    public void writePayloadIO() throws Exception {
        String testName = "payload-ioe";
        Path bag = out.resolve(testName);

        DirectoryPackager packager = startPackager();
        PackagerData data = packager.startBuild(testName);
        PayloadFile payload = new IOPayloadFile();

        HashCode hash = packager.writePayloadFile(payload, func, data);

        Path fileInBag = bag.resolve(payload.getFile());
        Assert.assertNull(hash);
        // Assert.assertFalse(Files.exists(fileInBag));
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

    private class IOPayloadFile extends PayloadFile {

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