package org.chronopolis.bag.writer;

import com.google.common.collect.ImmutableList;
import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.BagInfo;
import org.chronopolis.bag.core.BagIt;
import org.chronopolis.bag.core.PayloadFile;
import org.chronopolis.bag.core.PayloadManifest;
import org.chronopolis.bag.core.TagFile;
import org.chronopolis.bag.core.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

/**
 * Class to create bags from a set of payload files
 *
 * Created by shake on 11/14/16.
 */
@SuppressWarnings("WeakerAccess")
public class Bagger {
    private final Logger log = LoggerFactory.getLogger(Bagger.class);

    private double max = -1;
    private double threshold;

    private BagIt bagit;
    private BagInfo bagInfo;
    private NamingSchema namingSchema;
    private PayloadManifest payloadManifest;

    private Set<TagFile> tags;

    // Post write utils
    private boolean success = true;
    private List<PayloadFile> rejected;


    public Bagger() {
        bagit = new BagIt();
        bagInfo = new BagInfo();
        namingSchema = new UUIDNamingSchema();
        tags = new HashSet<>();
        rejected = new ArrayList<>();
    }

    // setters... wonder if there's a good way to move these out of this class

    public Bagger withMaxSize(int size, Unit unit) {
        this.max = unit.size() * size;
        this.threshold = max * 0.01;
        return this;
    }

    public Bagger withBagit(BagIt bagit) {
        this.bagit = bagit;
        return this;
    }

    public Bagger withBagInfo(BagInfo bagInfo) {
        this.bagInfo = bagInfo;
        return this;
    }

    public Bagger withNamingSchema(NamingSchema namingSchema) {
        this.namingSchema = namingSchema;
        return this;
    }

    public Bagger withPayloadManifest(PayloadManifest payloadManifest) {
        this.payloadManifest = payloadManifest;
        return this;
    }

    public Bagger withTagFile(TagFile tagFile) {
        tags.add(tagFile);
        return this;
    }

    // Method for creating bags from what we have
    public BaggingResult kraft() {
        log.info("Processing files. Max size is {} bytes", max);

        int idx = 0;
        boolean closed = false;

        List<Bag> finished = new ArrayList<>();
        Queue<Bag> processing = new PriorityQueue<>(new SizeComparator());

        Bag head = new Bag();
        head.setName(namingSchema.getName(idx));
        head.setMaxSize(max);
        processing.add(head);

        for (PayloadFile file : payloadManifest.getFiles().values()) {
            add(file, processing, finished);
        }

        finished.addAll(processing);
        updateMetadata(finished);
        return new BaggingResult(success, ImmutableList.copyOf(finished), ImmutableList.copyOf(rejected));
    }

    private void updateMetadata(List<Bag> finished) {
        for (Bag bag : finished) {
            int idx = finished.indexOf(bag);
            tags.forEach(bag::addTag);
            bag.setName(namingSchema.getName(idx));
            bag.setInfo(TagFile.copy(bagInfo));

            bag.setNumber(idx);
            bag.setGroupTotal(finished.size());
        }
    }

    private void add(PayloadFile file, Queue<Bag> processing, List<Bag> finished) {
        Bag current = processing.poll();
        log.info("Polling for bag... {}", current);

        // Are we out of bags?
        if (current == null) {
            // Short circuit
            if (file.getSize() > max) {
                success = false;
                rejected.add(file);
                log.warn("File is too large, rejecting ({})", file.getFile().toString());
                return;
            }

            current = new Bag();
            current.setMaxSize(max);
            current.setName(namingSchema.getName(0));
        }

        // Did we add the file?
        if (current.tryAdd(file)) {
            log.debug("Added file {} to {}", file.getFile(), current);
        } else {
            add(file, processing, finished);
        }

        // Can the current bag continue being processed?
        if (max != -1 && current.getSize() + threshold > current.getMaxSize()) {
            log.info("Threshold for {} met, closing", current);
            finished.add(current);
        } else if (!current.isEmpty()) {
            // log.info("Threshold for {} not met, adding back to processing queue", current);
            processing.offer(current);
        }
    }

    private class SizeComparator implements Comparator<Bag> {
        @Override
        public int compare(Bag lhs, Bag rhs) {
            return Long.compare(lhs.getSize(), rhs.getSize());
        }
    }
}
