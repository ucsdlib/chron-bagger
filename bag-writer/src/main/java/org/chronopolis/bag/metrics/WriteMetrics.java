package org.chronopolis.bag.metrics;

import java.util.HashSet;
import java.util.Set;

/**
 * Encapsulating class to contain information about the time to write for
 * a Bag, its manifests, and its payload files
 *
 * @author shake
 */
public class WriteMetrics {

    /**
     * Metric covering the total information about a bag
     */
    private Metric bag = new Metric();

    /**
     * Metric covering bagit.txt
     */
    private Metric bagIt = new Metric();

    /**
     * Metric covering bag-info.txt
     */
    private Metric bagInfo = new Metric();

    /**
     * Metric covering all payload file writes
     */
    private Metric payload;

    /**
     * Metric covering a single manifest-$alg.txt
     */
    private Metric manifest;

    /**
     * Metric covering a single tagmanifest-$alg.txt
     */
    private Metric tagmanifest;

    /**
     * Group to capture all extra tag files
     */
    private Set<Metric> extraTags;

    /**
     * Group to capture all payload file writes
     */
    private Set<Metric> payloadFiles;

    public WriteMetrics() {
        this.extraTags = new HashSet<>();
        this.payloadFiles = new HashSet<>();
    }

    public Metric getBag() {
        return bag;
    }

    public WriteMetrics setBag(Metric bag) {
        this.bag = bag;
        return this;
    }

    public Metric getBagIt() {
        return bagIt;
    }

    public WriteMetrics setBagIt(Metric bagIt) {
        this.bagIt = bagIt;
        return this;
    }

    public Metric getBagInfo() {
        return bagInfo;
    }

    public WriteMetrics setBagInfo(Metric bagInfo) {
        this.bagInfo = bagInfo;
        return this;
    }

    public Metric getPayload() {
        return payload;
    }

    public WriteMetrics setPayload(Metric payload) {
        this.payload = payload;
        return this;
    }

    public Metric getManifest() {
        return manifest;
    }

    public WriteMetrics setManifest(Metric manifest) {
        this.manifest = manifest;
        return this;
    }

    public Metric getTagmanifest() {
        return tagmanifest;
    }

    public WriteMetrics setTagmanifest(Metric tagmanifest) {
        this.tagmanifest = tagmanifest;
        return this;
    }

    public Set<Metric> getExtraTags() {
        return extraTags;
    }

    public WriteMetrics setExtraTags(Set<Metric> extraTags) {
        this.extraTags = extraTags;
        return this;
    }

    public WriteMetrics addTagMetric(Metric tag) {
        this.extraTags.add(tag);
        return this;
    }

    public Set<Metric> getPayloadFiles() {
        return payloadFiles;
    }

    public WriteMetrics setPayloadFiles(Set<Metric> payloadFiles) {
        this.payloadFiles = payloadFiles;
        return this;
    }

    public WriteMetrics addPayloadMetric(Metric payload) {
        this.payloadFiles.add(payload);
        return this;
    }
}
