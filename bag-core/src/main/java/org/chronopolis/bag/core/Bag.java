package org.chronopolis.bag.core;

import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * TODO: Should we have a notion of a closed bag and an open bag?
 *       Or maybe read only/some version of an immutable bag?
 * TODO: maxFiles
 *
 * Created by shake on 7/29/15.
 */
public class Bag {
    private final Logger log = LoggerFactory.getLogger(Bag.class);

	// Used as the base directory
    private String name;

    // Used for the PayloadOxum
    private long size = 0;
    private long numFiles = 0;
    private double maxSize = -1;

    // Used for the BagCount
    private int number = 0;
    private int groupTotal = 0;

    // Tag files
    // TODO: Separate fields for BagIt?
    private BagInfo info = new BagInfo();
    private Map<Path, TagFile> tags;

    // Payload files
    private Map<Path, PayloadFile> files;

    // Our two manifests
    private TagManifest tagManifest;
    private PayloadManifest manifest;

    // Post building/validation things
    private String receipt;
    private Set<PayloadFile> errors = new HashSet<>();
    private Set<String> buildErrors = new HashSet<>();

	public Bag() {
        this.tags = new HashMap<>();
        this.files = new HashMap<>();
		this.tagManifest = new TagManifest();
        this.manifest = new PayloadManifest();
	}

    public void prepareForWrite() {
        // Ensure these are all up to date
        setBaggingDate(LocalDate.now());

        // Set our payload oxum and size
        info.withInfo(BagInfo.Tag.INFO_PAYLOAD_OXUM, getPayloadOxum())
            .withInfo(BagInfo.Tag.INFO_BAG_SIZE, getFormattedSize());

        // Set group info if necessary
        if (getGroupTotal() > 1) {
            String bagCount = (getNumber() + 1) + " of " + getGroupTotal();
            getInfo().withInfo(BagInfo.Tag.INFO_BAG_COUNT, bagCount);
        }

        // Set group id if necessary
        if (getGroupId() != null && !getGroupId().isEmpty()) {
            getInfo().withInfo(BagInfo.Tag.INFO_BAG_GROUP_IDENTIFIER, getGroupId());
        }

        // Add the BagInfo to our tag files
        addTag(info);
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

	public double getMaxSize() {
        return maxSize;
    }

    public Bag setMaxSize(double maxSize) {
        this.maxSize = maxSize;
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
        int bytey = 1000;
        if (size < bytey) {
            return String.format("%s B", size);
        }
        // The natural log will show us how many digits we have
        // floor(log2(x)) = 63 - numberOfLeadingZeros(x)
        // div by 10 for our exp (K/M/G/...)
        int exp = (63 - Long.numberOfLeadingZeros(size)) / 10;

        return String.format("%.1f %sB", size / Math.pow(bytey, exp), "KMGTP".charAt(exp - 1));
    }

	public Map<Path, TagFile> getTags() {
		return tags;
	}

    /**
     * Replaces all current tag files with a copy of those found in tags
     *
     * @param tags the tag files to replace with
     * @return this bag
     */
	public Bag setTags(Map<Path, TagFile> tags) {
        if (this.tags == null) {
            this.tags = new HashMap<>();
        }

        this.tags = Maps.newHashMap(tags);
        return this;
	}

	public Bag addTag(TagFile tag) {
        TagFile copy = TagFile.copy(tag);
        if (tags == null) {
            tags = new HashMap<>();
        }

        this.tags.put(copy.getPath(), copy);
        return this;
	}
    
	public Map<Path, PayloadFile> getFiles() {
		return files;
	}

    /**
     * Replaces all payload files with a copy of the map passed in
     *
     * @param files the payload files to replace with
     * @return this bag
     */
	public Bag setFiles(Map<Path, PayloadFile> files) {
		this.files = Maps.newHashMap(files);
        this.numFiles = files.size();
        this.size = files.values().stream().reduce(0L,
                (l, payload) -> l + payload.getSize(),
                (l, r) -> l + r);
        return this;
	}

    public Bag addFile(PayloadFile file) { 
        if (files == null) {
            files = new HashMap<>();
        }

        this.files.put(file.getFile(), file);
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

    public void addFiles(Map<Path, PayloadFile> files) {
        if (this.files == null) {
            this.files = new HashMap<>();
        }

        // We could
        //   get the difference of the sets
        //   add the rhs easy
        //   compute differences in the intersection
        // OR
        //   just do this
        this.files.putAll(files);

        // update our sizes
        this.size = 0;
        numFiles = this.files.size();
        files.values().forEach(x -> this.size += x.getSize());
    }

    /**
     * Return any files which could not be validated with their checksum
     *
     * @return error'd PayloadFiles
     */
    public Set<PayloadFile> getErrors() {
        return errors;
    }

    public Set<String> getBuildErrors() {
        return buildErrors;
    }

    public Bag addError(PayloadFile error) {
        this.errors.add(error);
        return this;
    }

    public Bag addBuildError(String message) {
        this.buildErrors.add(message);
        return this;
    }

    public boolean isValid() {
        return errors.isEmpty() && buildErrors.isEmpty();
    }

    // Because the BagInfo only contains the string "x of n"
    // we need to keep track of these here and upate the BagInfo
    // when being written
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


    // Setters for fields kept within the BagInfo

    // TODO: Optional instead of null?
    public String getGroupId() {
        Collection<String> groupIds = info.getInfo(BagInfo.Tag.INFO_BAG_GROUP_IDENTIFIER);
        if (groupIds.isEmpty()) {
            return null;
        }

        Optional<String> first = groupIds.stream().findFirst();
        return first.orElse("");
    }

    public Bag setGroupId(String groupId) {
        info.withInfo(BagInfo.Tag.INFO_BAG_GROUP_IDENTIFIER, groupId);
        return this;
    }

    public LocalDate getBaggingDate() {
        Collection<String> dates = info.getInfo(BagInfo.Tag.INFO_BAGGING_DATE);
        if (dates.isEmpty()) {
            // We'll figure out something better to return
            return LocalDate.now();
        }

        return LocalDate.parse(dates.iterator().next());
    }

    public Bag setBaggingDate(LocalDate baggingDate) {
        String formattedDate = baggingDate.format(DateTimeFormatter.ISO_LOCAL_DATE);
        info.withInfo(BagInfo.Tag.INFO_BAGGING_DATE, formattedDate);
        return this;
    }

    public BagInfo getInfo() {
        return info;
    }

    public Bag setInfo(BagInfo info) {
        this.info = info;
        return this;
    }

    /**
     * Try to add a file to a bag
     * if successful, the manifest, and all metadata is updated to reflect
     * the current state of the bag
     *
     * @param file the file to add
     * @return success of adding the file
     */
    public boolean tryAdd(PayloadFile file) {
        if (maxSize == -1 || file.getSize() + size <= maxSize) {
            files.put(file.getFile(), file);
            manifest.addPayloadFile(file);

            ++numFiles;
            size += file.getSize();

            return true;
        }


        return false;
    }

    public boolean isEmpty() {
        return files.isEmpty();
    }
}
