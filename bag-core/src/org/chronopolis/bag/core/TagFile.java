package org.chronopolis.bag.core;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * Class so we can treat all Tag files the same
 *
 * Created by shake on 7/29/15.
 */
public interface TagFile {

    Path getPath();
    InputStream getInputStream();

}
