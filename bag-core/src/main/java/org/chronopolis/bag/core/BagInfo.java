package org.chronopolis.bag.core;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Map;

/**
 * Representation of the BagInfo file
 * <p/>
 * Created by shake on 7/29/15.
 */
public class BagInfo implements TagFile, Comparable<BagInfo> {

    // Constants
    public enum Tag {

        INFO_SOURCE_ORGANIZATION("Source-Organization"),
        INFO_ORGANIZATION_ADDRESS("Organization-Address"),
        INFO_CONTACT_NAME("Contact-Name"),
        INFO_CONTACT_PHONE("Contact-Phone"),
        INFO_CONTACT_EMAIL("Contact-Email"),
        INFO_EXTERNAL_DESCRIPTION("External-Description"),
        INFO_BAGGING_DATE("Bagging-Date"),
        INFO_EXTERNAL_IDENTIFIER("External-Identifier"),
        INFO_BAG_SIZE("Bag-Size"),
        INFO_PAYLOAD_OXUM("Payload-Oxum"),
        INFO_BAG_GROUP_IDENTIFIER("Bag-Group-Identifier"),
        INFO_BAG_COUNT("Bag-Count"),
        INFO_INTERNAL_SENDER_IDENTIFIER("Internal-Sender-Identifier"),
        INFO_INTERNAL_SENDER_DESCRIPTION("Internal-Sender-Description");

        private final String name;


        Tag(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }

    // Possibly use JodaTime instead
    private final Logger log = LoggerFactory.getLogger(BagInfo.class);
    private final Path path;

    /**
     * Map of the various Tags for a bag-info file
     *
     */
    private Multimap<Tag, String> tags = ArrayListMultimap.create();


    public BagInfo() {
        this.path = Paths.get("bag-info.txt");
    }

    public BagInfo withInfo(Tag identifier, String value) {
        tags.put(identifier, value);
        return this;
    }

    @Override
    public BagInfo clone() {
        BagInfo clone = new BagInfo();
        clone.tags.putAll(tags);
        return clone;
    }

    public Collection<String> getInfo(Tag identifier) {
        return tags.get(identifier);
    }

    @Override
    public long getSize() {
        long size = 0;
        for (Map.Entry<Tag, String> entry : tags.entries()) {
            StringBuilder tag = new StringBuilder();
            tag.append(entry.getKey().getName())
                    .append(": ")
                    .append(entry.getValue())
                    .append("\r\n");
            size += tag.toString().length();
        }
        return size;
    }

    @Override
    public Path getPath() {
        return path;
    }

    @Override
    public InputStream getInputStream() {
        // InputStream for writing tags
        PipedInputStream is = new PipedInputStream();
        // Output stream connected to the InputStream
        PipedOutputStream os = new PipedOutputStream();

        try {
            is.connect(os);
            // TODO: TagWriter which allows only 80 characters per line
            for (Map.Entry<Tag, String> entry : tags.entries()) {
                StringBuilder tag = new StringBuilder();
                tag.append(entry.getKey().getName())
                        .append(": ")
                        .append(entry.getValue())
                        .append("\r\n"); // TODO: Constant for carriage return?
                os.write(tag.toString().getBytes());
            }
            os.close();
        } catch (IOException e) {
            log.error("Error writing BagInfo InputStream", e);
        }
        return is;
    }

    @Override
    public int compareTo(BagInfo bagInfo) {
        if (this.equals(bagInfo)) {
            return 0;
        }

        return this.hashCode() - bagInfo.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BagInfo bagInfo = (BagInfo) o;

        return path != null ? path.equals(bagInfo.path) : bagInfo.path == null;

    }

    @Override
    public int hashCode() {
        return path != null ? path.hashCode() : 0;
    }
}
