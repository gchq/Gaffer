/*
 * Copyright 2016 Crown Copyright
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
package uk.gov.gchq.gaffer.accumulostore.utils;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.mock.MockConnector;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.CommonConstants;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.TreeSet;

/**
 * Utility methods for adding data to Accumulo.
 */
public final class IngestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(IngestUtils.class);
    private static final FsPermission ACC_DIR_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    private static final FsPermission ACC_FILE_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    private IngestUtils() {
        // private to prevent this class being instantiated.
        // All methods are static and should be called directly.
    }

    /**
     * Get the existing splits from a table in Accumulo and write a splits file.
     * The number of splits is returned.
     *
     * @param conn       - An existing connection to an Accumulo instance
     * @param table      - The table name
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @param maxSplits  - The maximum number of splits
     * @return The number of splits in the table
     * @throws IOException for any IO issues reading from the file system. Other accumulo exceptions are caught and wrapped in an IOException.
     */
    public static int createSplitsFile(final Connector conn, final String table, final FileSystem fs,
                                       final Path splitsFile, final int maxSplits) throws IOException {
        LOGGER.info("Creating splits file in location {} from table {} with maximum splits {}", splitsFile,
                table, maxSplits);
        // Get the splits from the table
        Collection<Text> splits;
        try {
            splits = conn.tableOperations().listSplits(table, maxSplits);
        } catch (final TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            throw new IOException(e.getMessage(), e);
        }
        // This should have returned at most maxSplits splits, but this is not implemented properly in MockInstance.
        if (splits.size() > maxSplits) {
            if (conn instanceof MockConnector) {
                LOGGER.info("Manually reducing the number of splits to {} due to MockInstance not implementing"
                        + " listSplits(table, maxSplits) properly", maxSplits);
            } else {
                LOGGER.info("Manually reducing the number of splits to {} (number of splits was {})", maxSplits, splits.size());
            }
            final Collection<Text> filteredSplits = new TreeSet<>();
            final int outputEveryNth = splits.size() / maxSplits;
            LOGGER.info("Outputting every {}-th split from {} total", outputEveryNth, splits.size());
            int i = 0;
            for (final Text text : splits) {
                if (i % outputEveryNth == 0) {
                    filteredSplits.add(text);
                }
                i++;
                if (filteredSplits.size() >= maxSplits) {
                    break;
                }
            }
            splits = filteredSplits;
        }
        LOGGER.info("Found {} splits from table {}", splits.size(), table);

        try (final PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsFile, true)), false,
                CommonConstants.UTF_8)) {
            // Write the splits to file
            if (splits.isEmpty()) {
                out.close();
                return 0;
            }

            for (final Text split : splits) {
                out.println(new String(Base64.encodeBase64(split.getBytes()), CommonConstants.UTF_8));
            }
        }
        return splits.size();
    }

    public static int createSplitsFile(final Connector conn, final String table, final FileSystem fs,
                                       final Path splitsFile) throws IOException {
        return createSplitsFile(conn, table, fs, splitsFile, Integer.MAX_VALUE);
    }

    /**
     * Read a splits file and get the number of split points within
     *
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @return An integer representing the number of entries in the file.
     * @throws IOException for any IO issues reading from the file system.
     */
    @SuppressFBWarnings(value = "RV_DONT_JUST_NULL_CHECK_READLINE", justification = "Simply counts the number of lines in the file")
    public static int getNumSplits(final FileSystem fs, final Path splitsFile) throws IOException {
        int numSplits = 0;
        try (final FSDataInputStream fis = fs.open(splitsFile);
             final InputStreamReader streamReader = new InputStreamReader(fis, CommonConstants.UTF_8);
             final BufferedReader reader = new BufferedReader(streamReader)) {
            while (null != reader.readLine()) {
                ++numSplits;
            }
        }

        return numSplits;
    }

    /**
     * Modify the permissions on a directory and its contents to allow Accumulo
     * access.
     * <p>
     *
     * @param fs      - The FileSystem in which to create the splits file
     * @param dirPath - The Path to the directory
     * @throws IOException for any IO issues interacting with the file system.
     */

    public static void setDirectoryPermsForAccumulo(final FileSystem fs, final Path dirPath) throws IOException {
        if (!fs.getFileStatus(dirPath).isDirectory()) {
            throw new RuntimeException(dirPath + " is not a directory");
        }
        LOGGER.info("Setting permission {} on directory {} and all files within", ACC_DIR_PERMS, dirPath);
        fs.setPermission(dirPath, ACC_DIR_PERMS);
        for (final FileStatus file : fs.listStatus(dirPath)) {
            fs.setPermission(file.getPath(), ACC_FILE_PERMS);
        }
    }
}
