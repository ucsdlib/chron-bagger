package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.*;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Future;

/**
 * Main class to create bags from
 *
 * Created by shake on 7/30/2015.
 */
public abstract class Writer {

    public Writer() {
    }

    public abstract Writer validate(boolean validate);
    public abstract Writer withDigest(Digest digest);
    public abstract Writer withMaxSize(Unit maxSize);
    public abstract Writer withNamingSchema(NamingSchema namingSchema);

    // tar/dir/zip
    public abstract Writer withPackager(Packager packager);

    public abstract Writer withBagIt(BagIt bagIt);
    public abstract Writer withTagFile(TagFile file);
    public abstract Writer withBagInfo(BagInfo bagInfo);

    public abstract Writer withPayloadFile(PayloadFile file);

    // TODO: PayloadDirectory
    public abstract Writer withPayloadDirectory(Path directory);

    public abstract Writer withPayloadManifest(PayloadManifest manifest);

    @Deprecated
    public abstract Writer preserveManifest(boolean preserve);

    public abstract List<Bag> write();
    public abstract List<Future<Bag>> writeAsync();

}
