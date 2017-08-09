/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.parquetstore.ParquetStoreProperties;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParquetFileIteratorTest {
    private FileSystem fs;
    private ParquetFileIterator iterator;
    private static final ParquetStoreProperties pp = new ParquetStoreProperties();
    private static String rootDir = pp.getTempFilesDir();

    private final Path TEST_1 = new Path(rootDir + "/test.parquet");
    private final Path TEST_2 = new Path(rootDir + "/test2.parquet");
    private final Path TEST_3 = new Path(rootDir + "/test.txt");


    @Before
    public void setUp() throws IOException {
        Logger.getRootLogger().setLevel(Level.WARN);
        fs = FileSystem.get(new Configuration());
        fs.delete(new Path(rootDir), true);
        fs.mkdirs(new Path(rootDir));
        fs.create(TEST_1);
        fs.create(TEST_2);
        fs.create(TEST_3);
        iterator = new ParquetFileIterator(new Path(rootDir), fs);
    }

    @After
    public void cleanUp() throws IOException {
        Path tempDir = new Path(rootDir);
        fs.delete(tempDir, true);
        while (fs.listStatus(tempDir.getParent()).length == 0) {
            tempDir = tempDir.getParent();
            fs.delete(tempDir, true);
        }
        fs.close();
        fs = null;
        rootDir = null;
        iterator = null;
    }

    @Test
    public void skipsNonParquetFiles() throws IOException {
        final List<Path> pathList = new ArrayList<>();
        iterator.forEachRemaining(pathList::add);

        assertFalse(pathList.isEmpty());
        assertTrue(pathList.contains(fs.resolvePath(TEST_1)));
        assertTrue(pathList.contains(fs.resolvePath(TEST_2)));
        assertFalse(pathList.contains(fs.resolvePath(TEST_3)));
    }
}
