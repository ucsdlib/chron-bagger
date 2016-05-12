package org.chronopolis.bag.core;

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * Created by shake on 5/12/16.
 */
public class BagTest {
    final TestPayloadFile testFile = new TestPayloadFile(Paths.get("some-payload-file"), 1L);
    final TestPayloadFile testFile2 = new TestPayloadFile(Paths.get("some-other-payload-file"), 5L);
    final TestPayloadFile testFile3 = new TestPayloadFile(Paths.get("some-large-payload-file"), 5000L);

    @Test
    public void testBagBasic() {
        TagManifest tagManifest = new TagManifest();
        PayloadManifest payloadManifest = new PayloadManifest();
        LocalDate baggingDate = LocalDate.now().minus(1, ChronoUnit.DAYS);

        Bag b = new Bag();

        // Things in the bag-info
        b.setBaggingDate(baggingDate);
        b.setGroupId("group-id-test");
        Assert.assertEquals(b.getBaggingDate(), baggingDate);
        Assert.assertEquals(b.getGroupId(), "group-id-test");

        // Set a new bag info and assert the past values have been forgotten
        b.setInfo(new BagInfo());
        Assert.assertEquals(b.getBaggingDate(), LocalDate.now());
        Assert.assertEquals(b.getGroupId(), null);

        // Other fields
        b.setSize(0);
        b.setNumber(1);
        b.setNumFiles(0);
        b.setGroupTotal(1);
        b.setName("test-bag");
        b.setReceipt("test-receipt");
        b.setTags(ImmutableMap.of());
        b.setFiles(ImmutableMap.of());
        b.setTagManifest(tagManifest);
        b.setManifest(payloadManifest);

        Assert.assertEquals(b.getGroupTotal(), 1);
        Assert.assertEquals(b.getInfo(), new BagInfo());
        Assert.assertEquals(b.getTagManifest(), tagManifest);
        Assert.assertEquals(b.getManifest(), payloadManifest);
        Assert.assertEquals(b.getName(), "test-bag");
        Assert.assertEquals(b.getTags(), ImmutableMap.of());
        Assert.assertEquals(b.getReceipt(), "test-receipt");
        Assert.assertEquals(b.getSize(), 0);
        Assert.assertEquals(b.getNumFiles(), 0);
        Assert.assertEquals(b.getNumber(), 1);
        Assert.assertEquals(b.getFiles(), ImmutableMap.of());
    }

    @Test
    public void testBagTagOps() {
        Map<Path, TagFile> testMap = ImmutableMap.of(
                Paths.get("bagit.txt"), new BagIt(),
                Paths.get("only-a-test.txt"), new TestTagFile());

        Bag b = new Bag();
        b.addTag(new BagIt());

        Assert.assertEquals(1, b.getTags().size());

        b.setTags(testMap);
        Assert.assertEquals(2, b.getTags().size());
    }

    @Test
    public void testBagPayloadOps() {
        Map<Path, PayloadFile> files = ImmutableMap.of(
                testFile.getFile(), testFile,
                testFile2.getFile(), testFile2
        );

        Bag b = new Bag();
        // First have our files be from our set
        b.setFiles(files);
        Assert.assertEquals(6L, b.getSize());
        Assert.assertEquals(2L, b.getNumFiles());

        // Then reset
        b.setFiles(new HashMap<>());

        // Add a single file
        b.addFile(testFile);
        Assert.assertEquals(testFile.getSize(), b.getSize());
        Assert.assertEquals(1, b.getNumFiles());

        // Add our set again with a conflicting file
        b.addFiles(files);
        Assert.assertEquals(6L, b.getSize());
        Assert.assertEquals(2L, b.getNumFiles());
        Assert.assertEquals(2, b.getFiles().size());
        // 6 bytes && 6 bytes 2 files
        Assert.assertEquals("6 B", b.getFormattedSize());
        Assert.assertEquals("6.2", b.getPayloadOxum());
    }

    @Test
    public void testBagPayloadErrors() {
        Bag b = new Bag();

        // Test the error conditions
        b.addError(testFile);
        Assert.assertTrue(b.getErrors().contains(testFile));
        Assert.assertFalse( b.isValid());
    }

    @Test
    public void testPrepareForWrite() {
        Map<Path, PayloadFile> files = ImmutableMap.of(
                testFile.getFile(), testFile,
                testFile2.getFile(), testFile2,
                testFile3.getFile(), testFile3
        );
        Bag b = new Bag();
        b.addFiles(files);
        b.setGroupTotal(2);
        b.setGroupId("test-group");
        b.prepareForWrite();

        // A bit of weird way to go about it but since it's a multimap just use contains
        BagInfo info = b.getInfo();
        Assert.assertTrue(info.getInfo(BagInfo.Tag.INFO_BAGGING_DATE).contains(LocalDate.now().toString()));
        Assert.assertTrue(info.getInfo(BagInfo.Tag.INFO_PAYLOAD_OXUM).contains("5006.3"));
        Assert.assertTrue(info.getInfo(BagInfo.Tag.INFO_BAG_SIZE).contains("5.0 KB"));
        Assert.assertTrue(info.getInfo(BagInfo.Tag.INFO_BAG_COUNT).contains("1 of 2"));
        Assert.assertTrue(info.getInfo(BagInfo.Tag.INFO_BAG_GROUP_IDENTIFIER).contains("test-group"));
    }

    private class TestTagFile implements TagFile {
        @Override
        public long getSize() {
            return 0;
        }

        @Override
        public Path getPath() {
            return Paths.get("only-a-test.txt");
        }

        @Override
        public InputStream getInputStream() {
            return new ByteArrayInputStream("test".getBytes());
        }
    }

    private class TestPayloadFile extends PayloadFile {

        long size;
        Path file;

        TestPayloadFile(Path file, long size) {
            this.file = file;
            this.size = size;
        }

        @Override
        public Path getFile() {
            return file;
        }

        @Override
        public long getSize() {
            return size;
        }

    }


}