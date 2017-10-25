/*
 * Copyright 2017 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.gov.gchq.gaffer.hbasestore.utils;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Utility methods for adding data to HBase.
 */
public final class IngestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);
    private static final FsPermission HBASE_DIR_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    private static final FsPermission HBASE_FILE_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private IngestUtils() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Modify the permissions on a directory and its contents to allow HBase
     * access.
     * <p>
     *
     * @param fs      - The FileSystem in which to create the splits file
     * @param dirPath - The path to the directory
     * @throws IOException for any IO issues interacting with the file system.
     */
    public static void setDirectoryPermsForHbase(final FileSystem fs, final Path dirPath) throws IOException {
        if (!fs.getFileStatus(dirPath).isDirectory()) {
            throw new RuntimeException(dirPath + " is not a directory");
        }
        LOGGER.info("Setting permission {} on directory {} and all files within", HBASE_DIR_PERMS, dirPath);
        fs.setPermission(dirPath, HBASE_DIR_PERMS);
        for (final FileStatus file : fs.listStatus(dirPath)) {
            fs.setPermission(file.getPath(), HBASE_FILE_PERMS);
        }
    }
}
