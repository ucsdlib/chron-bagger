package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Create bags using a limit
 *
 * Created by shake on 2/11/16.
 */
public class MultipartWriter extends SimpleWriter {
    private final Logger log = LoggerFactory.getLogger(MultipartWriter.class);

    private double max;
    private List<Bag> bags;

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
            writeBag(bag, idx);
            idx++;
        }

        return bags;
    }

    /**
     * Create buckets for our PayloadFiles so we can split them
     *
     */
    private void preprocess() {
        log.info("Processing files. Max size is {} bytes", max);

        Set<PayloadFile> files = b.getFiles();
        int idx = 0;
        boolean closed = false;

        Bag current = new Bag();
        PayloadManifest currentManifest = new PayloadManifest();

        for (PayloadFile file : files) {
            closed = false;
            current.addFile(file);
            currentManifest.addPayloadFile(file);

            if (current.getSize() > max) {
                finishProcessing(current, currentManifest, idx++);

                closed = true;
                current = new Bag();
                currentManifest = new PayloadManifest();
            }
        }

        log.info("count {} size {}", closed, files.size());
        // Close out the final bag
        if (!closed) {
            finishProcessing(current, currentManifest, idx);
        }
    }

    private void finishProcessing(Bag bag, PayloadManifest manifest, int idx) {
        bag.setNumber(idx);
        bag.setTags(b.getTags());
        bag.setInfo(b.getInfo().clone());
        bag.setManifest(manifest);
        bags.add(bag);

        log.info("Completed bag {} (sizeof={})", idx, bag.getSize());
    }

}
