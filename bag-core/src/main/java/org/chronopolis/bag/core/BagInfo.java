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
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Map;

/**
 * Representation of the BagInfo file
 * <p/>
 * Created by shake on 7/29/15.
 */
public class BagInfo implements TagFile {

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
    public static DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final Path path;

    /**
     * Map of the various Tags for a bag-info file
     *
     */
    private Multimap<Tag, String> tags = ArrayListMultimap.create();

    /**
     * InputStream for writing tags
     *
     */
    private PipedInputStream is;

    /**
     * Output stream connected to the InputStream
     *
     */
    private PipedOutputStream os;


    public BagInfo() {
        this.path = Paths.get("bag-info.txt");
    }

    public BagInfo withInfo(Tag identifier, String value) {
        tags.put(identifier, value);
        return this;
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
        is = new PipedInputStream();
        os = new PipedOutputStream();

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
}
