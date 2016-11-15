package org.chronopolis.bag.writer;

import org.chronopolis.bag.core.Bag;
import org.chronopolis.bag.core.PayloadFile;

import java.util.List;

/**
 * Class to encapsulate the result of creating a set of bags from a payload manifest
 *
 * Created by shake on 11/15/16.
 */
public class BaggingResult {

    final boolean success;
    final List<Bag> bags;
    final List<PayloadFile> rejected;

    public BaggingResult(boolean success, List<Bag> bags, List<PayloadFile> rejected) {
        this.success = success;
        this.bags = bags;
        this.rejected = rejected;
    }
}
