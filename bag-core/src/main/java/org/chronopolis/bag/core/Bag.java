package org.chronopolis.bag.core;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * Created by shake on 7/29/15.
 */
public class Bag {

	// Used as the base directory
    private String name;

    // Some metadata
    // TODO: Keep info about tag files as well?
    private long size = 0;
    private long numFiles = 0;

    private int number;
    private int groupTotal;
    private String groupId;
    private Date baggingDate;

    // Tag files
    // TODO: Separate fields for BagIt/BagInfo?
    private Set<TagFile> tags;

    // Payload files
    private Set<PayloadFile> files;

    // Our two manifests
    private TagManifest tagManifest;
    private PayloadManifest manifest;

    // Post building/validation things
    private String receipt;
    private Set<PayloadFile> errors = new HashSet<>();

	public Bag() {
        this.tags = new HashSet<>();
		this.tagManifest = new TagManifest();
	}

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
	 * @return size of the bag
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

    public String getPayloadOxum() {
        return String.valueOf(size) +
                "." +
                numFiles;
    }

    public String getFormattedSize() {
        return null;
    }

	public Set<TagFile> getTags() {
		return tags;
	}

	public Bag setTags(Set<TagFile> tags) {
		this.tags = tags;
        return this;
	}

	public Bag addTag(TagFile tag) {
        if (tags == null) {
            tags = new HashSet<>();
        }
		this.tags.add(tag);
        return this;
	}
    
	public Set<PayloadFile> getFiles() {
		return files;
	}

	public Bag setFiles(Set<PayloadFile> files) {
		this.files = files;
        return this;
	}

    public Bag addFile(PayloadFile file) { 
        if (files == null) {
            files = new HashSet<>();
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
     * @param receipt the bags receipt
     * @return the bag object
     */
	public Bag setReceipt(String receipt) {
		this.receipt = receipt;
        return this;
	}

    public void addFiles(Set<PayloadFile> files) {
        if (this.files == null) {
            this.files = new HashSet<>();
        }

        this.files.addAll(files);
    }

    /**
     * Return any files which could not be validated with their checksum
     *
     * @return error'd PayloadFiles
     */
    public Set<PayloadFile> getErrors() {
        return errors;
    }

    public Bag addError(PayloadFile error) {
        this.errors.add(error);
        return this;
    }

    public boolean isValid() {
        return errors.isEmpty();
    }

    public int getNumber() {
        return number;
    }

    public Bag setNumber(int number) {
        this.number = number;
        return this;
    }

    public int getGroupTotal() {
        return groupTotal;
    }

    public Bag setGroupTotal(int groupTotal) {
        this.groupTotal = groupTotal;
        return this;
    }

    public String getGroupId() {
        return groupId;
    }

    public Bag setGroupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    public Date getBaggingDate() {
        return baggingDate;
    }

    public Bag setBaggingDate(Date baggingDate) {
        this.baggingDate = baggingDate;
        return this;
    }

}
