package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Test interface for a new writer
 *
 * Created by shake on 11/16/16.
 */
public interface ProtoWriter {

    ProtoWriter validate(boolean validate);
    ProtoWriter withPackager(Packager packager);

    List<CompletableFuture<WriteResult>> write(List<Bag> bags);
    List<CompletableFuture<WriteResult>> write(List<Bag> bags, Executor executor);

}
