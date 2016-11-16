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
@Deprecated
public abstract class Writer {

    public Writer() {
    }

    public abstract Writer validate(boolean validate);
    public abstract Writer withDigest(Digest digest);         // Part of the bag
    public abstract Writer withMaxSize(int size, Unit unit);  // Part of the bag
    public abstract Writer withNamingSchema(NamingSchema namingSchema); // Part of the BagPartitioner

    // tar/dir/zip
    public abstract Writer withPackager(Packager packager); // Keep

    public abstract Writer withBagIt(BagIt bagIt);  // Part of the BagPartitioner
    public abstract Writer withTagFile(TagFile file);  // Part of the BagPartitioner
    public abstract Writer withBagInfo(BagInfo bagInfo);  // Part of the BagPartitioner

    public abstract Writer withPayloadFile(PayloadFile file);  // Part of the BagPartitioner?? Maybe need a PayloadManifestBuilder

    // TODO: PayloadDirectory
    public abstract Writer withPayloadDirectory(Path directory); // Neither

    public abstract Writer withPayloadManifest(PayloadManifest manifest); // Part of the BagPartitioner

    /**
     * @deprecated something of substance about why we don't use this
     */
    @Deprecated
    public abstract Writer preserveManifest(boolean preserve); // Not keeping going forward

    public abstract List<Bag> write(); // Return a result
    public abstract List<Future<Bag>> writeAsync(); // Combine with previous? If only we had implicits... oh well

}
