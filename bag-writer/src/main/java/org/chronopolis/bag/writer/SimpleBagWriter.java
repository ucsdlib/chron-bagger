package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.packager.Packager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 *
 * Created by shake on 11/16/16.
 */
public class SimpleBagWriter implements BagWriter {
    private final Logger log = LoggerFactory.getLogger(SimpleBagWriter.class);

    private boolean validate;
    private Packager packager;

    public SimpleBagWriter() {
    }

    @Override
    public BagWriter validate(boolean validate) {
        this.validate = validate;
        return this;
    }

    @Override
    public BagWriter withPackager(Packager packager) {
        this.packager = packager;
        return this;
    }

    @Override
    public List<WriteResult> write(List<Bag> bags) {
        checkNull(bags);
        return bags.stream()
                .map(this::fromBag)
                .collect(Collectors.toList());
    }

    // Just a helper so we have a clean map above
    private WriteResult fromBag(Bag bag) {
        WriteJob job = new WriteJob(bag, validate, packager);
        return job.get();
    }

    @Override
    public List<CompletableFuture<WriteResult>> write(List<Bag> bags, Executor executor) {
        checkNull(bags);
        return bags.stream()
                .map(b -> CompletableFuture.supplyAsync(new WriteJob(b, validate, packager), executor))
                .collect(Collectors.toList());
    }

    private void checkNull(List<Bag> bags) {
        if (bags == null) {
            throw new IllegalArgumentException("Bags cannot be null");
        }
    }
}
