package org.chronopolis.bag.core;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * Created by shake on 7/29/15.
 */
public class Bag {

	// Used as the base directory
    private String name;

    // Some metadata
    private long size = 0;
    private long numFiles = 0;

    // Tag files
    private List<TagFile> tags;

    // Payload files
    private List<PayloadFile> files;

    // Our two manifests
    private TagManifest tagManifest;
    private PayloadManifest manifest;

    // Receipt after building
    private String receipt;

    public String getName() {
		return name;
	}

	public Bag setName(String name) {
		this.name = name;
        return this;
	}

	/**
	 * Return the size of the bags payload directory
	 *
	 * @return
     */
	public long getSize() {
		return size;
	}

	public Bag setSize(long size) {
		this.size = size;
        return this;
	}

	public long getNumFiles() {
		return numFiles;
	}

	public Bag setNumFiles(long numFiles) {
		this.numFiles = numFiles;
        return this;
	}

	public List<TagFile> getTags() {
		return tags;
	}

	public Bag setTags(List<TagFile> tags) {
		this.tags = tags;
        return this;
	}

	public Bag addTag(TagFile tag) {
        if (tags == null) {
            tags = new ArrayList<>(); 
        }
		this.tags.add(tag);
        return this;
	}
    
	public List<PayloadFile> getFiles() {
		return files;
	}

	public Bag setFiles(List<PayloadFile> files) {
		this.files = files;
        return this;
	}

    public Bag addFile(PayloadFile file) { 
        if (files == null) {
            files = new ArrayList<>();
        }

        this.files.add(file);
        this.numFiles++;
        this.size += file.getSize();
        return this;
    }

	public TagManifest getTagManifest() {
		return tagManifest;
	}

	public Bag setTagManifest(TagManifest tagManifest) {
		this.tagManifest = tagManifest;
        return this;
	}

	public PayloadManifest getManifest() {
		return manifest;
	}

	public Bag setManifest(PayloadManifest manifest) {
		this.manifest = manifest;
        return this;
	}

	public String getReceipt() {
		return receipt;
	}

    /**
     * Set a receipt for the bag to use after building has completed
     *
     * @param receipt
     * @return
     */
	public Bag setReceipt(String receipt) {
		this.receipt = receipt;
        return this;
	}

}
