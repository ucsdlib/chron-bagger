package org.chronopolis.bag.core;

import java.util.List;

/**
 *
 * Created by shake on 7/29/15.
 */
public class Bag {

    // Used as the base directory
    private String name;

    // Some metadata
    private long size;
    private long numFiles;

    // Tag files
    private List<TagFile> tags;

    // Payload files
    private List<PayloadFile> files;

    // Our two manifests
    private TagManifest tagManifest;
    private PayloadManifest manifest;

    // Receipt after building
    private String receipt;

}
