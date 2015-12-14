/**
 * Copyright 2015 Crown Copyright
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
package gaffer.accumulo.utils;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.Collection;
import java.util.SortedSet;
import java.util.TreeSet;

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
     * @param conn  An existing connection to an Accumulo instance
     * @param table  The table name
     * @param fs  The FileSystem in which to create the splits file
     * @param splitsFile  A path for the splits file
     * @return The number of splits in the table
     * @throws TableNotFoundException
     * @throws IOException
     */
    public static int createSplitsFile(Connector conn, String table, FileSystem fs, Path splitsFile)
            throws TableNotFoundException, IOException {
        // Get the splits from the table
        Collection<Text> splits = conn.tableOperations().getSplits(table);

        // Write the splits to file
        if (splits.isEmpty()) {
            return 0;
        }
        PrintStream out = new PrintStream(new BufferedOutputStream(fs.create(splitsFile, true)));
        for (Text split : splits) {
            out.println(new String(Base64.encodeBase64(split.getBytes())));
        }
        out.close();

        return splits.size();
    }

    /**
     * Given some split points, write a Base64 encoded splits file.
     * 
     * @param splits  The split points
     * @param fs  The FileSystem in which to create the splits file
     * @param splitsFile  The location of the output splits file
     * @throws IOException
     */
    public static void writeSplitsFile(Collection<Text> splits, FileSystem fs, Path splitsFile) throws IOException {
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
     * Read a splits file and get the number of split points within.
     * 
     * @param fs  The FileSystem which contains the splits file
     * @param splitsFile  The Path to the splits file
     * @return The number of split points
     * @throws IOException
     */
    public static int getNumSplits(FileSystem fs, Path splitsFile) throws IOException {
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
     * @param fs  The FileSystem which contains the splits file
     * @param splitsFile  The Path to the splits file
     * @return
     * @throws IOException
     */
    public static SortedSet<Text> getSplitsFromFile(FileSystem fs, Path splitsFile) throws IOException {
        FSDataInputStream fis = fs.open(splitsFile);
        BufferedReader reader = new BufferedReader(new InputStreamReader(fis));
        SortedSet<Text> splits = new TreeSet<Text>();
        String line = null;
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
     * @param fs  The FileSystem where the directory is
     * @param dirPath  The path to the directory
     * @throws IOException
     */
    public static void setDirectoryPermsForAccumulo(FileSystem fs, Path dirPath) throws IOException {
        if (!fs.getFileStatus(dirPath).isDir()) {
            throw new RuntimeException(dirPath + " is not a directory");
        }
        fs.setPermission(dirPath, ACC_DIR_PERMS);
        for (FileStatus file : fs.listStatus(dirPath)) {
            fs.setPermission(file.getPath(), ACC_FILE_PERMS);
        }
    }
}
