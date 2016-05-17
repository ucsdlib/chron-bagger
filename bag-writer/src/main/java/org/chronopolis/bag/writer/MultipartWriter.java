package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.chronopolis.bag.core.TagFile.copy;

/**
 * Create bags using a limit
 *
 * Created by shake on 2/11/16.
 */
public class MultipartWriter extends SimpleWriter {
    private final Logger log = LoggerFactory.getLogger(MultipartWriter.class);

    private double max;
    protected List<Bag> bags;

    public MultipartWriter() {
        super();
        bags = new ArrayList<>();
    }

    @Override
    public MultipartWriter withMaxSize(int size, Unit unit) {
        this.max = size * unit.size();
        return this;
    }

    public List<Bag> write() {
        preprocess();
        int idx = 0;
        int total = bags.size();
        for (Bag bag : bags) {
            bag.setGroupTotal(total);
            bag.prepareForWrite();

            String name = namingSchema.getName(idx);
            bag.setName(name);

            writeBag(bag);
            idx++;
        }

        return bags;
    }

    /**
     * Create buckets for our PayloadFiles so we can split them
     *
     */
    protected void preprocess() {
        log.info("Processing files. Max size is {} bytes", max);

        int idx = 0;
        boolean closed = false;

        // TODO: Why not just clone the root bag? Then we don't need to worry about
        //       adding all the tag files and what not
        Bag current = new Bag();
        PayloadManifest currentManifest = new PayloadManifest();

        // TODO: This should fail if a file is greater than the max;
        //       and only allow UP TO max for a bag, not over
        for (PayloadFile file : b.getFiles().values()) {
            closed = false;
            current.addFile(file);
            currentManifest.addPayloadFile(file);

            if (current.getSize() >= max) {
                finishProcessing(current, currentManifest, idx++);

                closed = true;
                current = new Bag();
                currentManifest = new PayloadManifest();
            }
        }

        // Close out the final bag
        if (!closed) {
            finishProcessing(current, currentManifest, idx);
        }
    }

    private void finishProcessing(Bag bag, PayloadManifest manifest, int idx) {
        bag.setNumber(idx);

        // Copies of our tag files + bag info
        b.getTags().forEach((path, tagFile) -> bag.addTag(copy(tagFile)));
        bag.setInfo(copy(b.getInfo()));

        // manifest is unique so we don't need to make a separate copy
        bag.setManifest(manifest);

        bags.add(bag);
        log.info("Completed bag {} (sizeof={})", idx, bag.getSize());
    }

}
