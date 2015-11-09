package org.chronopolis.bag.core;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 *
 * Created by shake on 7/30/2015.
 */
public enum Digest {
    SHA_256(Hashing.sha256(), "sha256"),
    MD5(Hashing.md5(), "md5");

    private final HashFunction hashFunction;
    private final String bagFormattedName;

    Digest(HashFunction hashFunction, String bagFormattedName) {
        this.hashFunction = hashFunction;
        this.bagFormattedName = bagFormattedName;
    }

    public HashFunction getHashFunction() {
        return hashFunction;
    }

    public String getBagFormattedName() {
        return bagFormattedName;
    }
}
