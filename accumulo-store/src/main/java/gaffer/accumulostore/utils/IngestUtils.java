/*
 * Copyright 2016 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gaffer.accumulostore.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;

/**
 * Utility methods for adding data to Accumulo.
 */
public class IngestUtils {

    private static final FsPermission ACC_DIR_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);
    private static final FsPermission ACC_FILE_PERMS = new FsPermission(FsAction.ALL, FsAction.ALL, FsAction.ALL);

    /**
     * Get the existing splits from a table in Accumulo and write a splits file.
     * The number of splits is returned.
     *
     * @param conn       - An existing connection to an Accumulo instance
     * @param table      - The table name
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @return The number of splits in the table
     * @throws java.io.IOException
     */
    public static int createSplitsFile(final Connector conn, final String table, final FileSystem fs, final Path splitsFile)
            throws IOException {
        // Get the splits from the table
        Collection<Text> splits;
        try {
            splits = conn.tableOperations().listSplits(table);
        } catch (TableNotFoundException | AccumuloSecurityException | AccumuloException e) {
            throw new IOException(e.getMessage(), e);
        }

        PrintStream out;
        try {
            out = new PrintStream(new BufferedOutputStream(fs.create(splitsFile, true)));
        } catch (IOException e) {
            throw new IOException(e.getMessage(), e);
        }
        // Write the splits to file
        if (splits.isEmpty()) {
            out.close();
            return 0;
        }

        for (Text split : splits) {
            out.println(new String(Base64.encodeBase64(split.getBytes())));
        }
        out.close();

        return splits.size();
    }

    /**
     * Given some split points, write a Base64 encoded splits file
     *
     * @param splits     - A Collection of splits
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @throws java.io.IOException
     */
    public static void writeSplitsFile(final Collection<Text> splits, final FileSystem fs, final Path splitsFile) throws IOException {
        PrintStream out = null;
        try {
            out = new PrintStream(new BufferedOutputStream(fs.create(splitsFile, true)));
            for (Text split : splits) {
                out.println(new String(Base64.encodeBase64(split.getBytes())));
            }
        } finally {
            IOUtils.closeStream(out);
        }
    }

    /**
     * Read a splits file and get the number of split points within
     *
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @return An integer representing the number of entries in the file.
     * @throws java.io.IOException
     */
    public static int getNumSplits(final FileSystem fs, final Path splitsFile) throws IOException {
        FSDataInputStream fis = fs.open(splitsFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        int numSplits = 0;
        while (reader.readLine() != null) {
            ++numSplits;
        }
        reader.close();
        return numSplits;
    }

    /**
     * Read a Base64 encoded splits file and return the splits as Text objects
     *
     * @param fs         - The FileSystem in which to create the splits file
     * @param splitsFile - A Path for the output splits file
     * @return A set of Text objects representing the locations of split points in HDFS
     * @throws java.io.IOException
     */
    public static SortedSet<Text> getSplitsFromFile(final FileSystem fs, final Path splitsFile) throws IOException {
        FSDataInputStream fis = fs.open(splitsFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        SortedSet<Text> splits = new TreeSet<>();
        String line;
        while ((line = reader.readLine()) != null) {
            splits.add(new Text(Base64.decodeBase64(line)));
        }
        reader.close();
        return splits;
    }

    /**
     * Modify the permissions on a directory and its contents to allow Accumulo
     * access.
     *
     * @param fs      - The FileSystem in which to create the splits file
     * @param dirPath - The Path to the directory
     * @throws java.io.IOException
     */
    public static void setDirectoryPermsForAccumulo(final FileSystem fs, final Path dirPath) throws IOException {
        if (!fs.getFileStatus(dirPath).isDirectory()) {
            throw new RuntimeException(dirPath + " is not a directory");
        }
        fs.setPermission(dirPath, ACC_DIR_PERMS);
        for (FileStatus file : fs.listStatus(dirPath)) {
            fs.setPermission(file.getPath(), ACC_FILE_PERMS);
        }
    }
}