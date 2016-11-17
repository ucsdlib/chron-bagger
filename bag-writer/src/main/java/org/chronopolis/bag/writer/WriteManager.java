package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.packager.Packager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * Created by shake on 11/16/16.
 */
public class WriteManager implements ProtoWriter {
    private final Logger log = LoggerFactory.getLogger(WriteManager.class);

    private boolean validate;
    private Packager packager;

    public WriteManager() {
    }

    @Override
    public ProtoWriter validate(boolean validate) {
        this.validate = validate;
        return this;
    }

    @Override
    public ProtoWriter withPackager(Packager packager) {
        this.packager = packager;
        return this;
    }

    @Override
    public List<CompletableFuture<WriteResult>> write(List<Bag> bags) {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            return write(bags, executor);
        } finally {
           executor.shutdown();
        }
    }

    @Override
    public List<CompletableFuture<WriteResult>> write(List<Bag> bags, Executor executor) {
        List<CompletableFuture<WriteResult>> futures = new ArrayList<>();
        for (Bag bag : bags) {
            WriteJob job = new WriteJob(bag, validate, packager);
            CompletableFuture<WriteResult> future = CompletableFuture.supplyAsync(job, executor);
            futures.add(future);
        }

        return futures;
    }
}
