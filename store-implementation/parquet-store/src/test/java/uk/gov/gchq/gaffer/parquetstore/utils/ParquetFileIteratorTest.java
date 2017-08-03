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

import static org.junit.Assert.assertEquals;

public class ParquetFileIteratorTest {
    private FileSystem fs;
    private String rootDir;
    private ParquetFileIterator iterator;

    @Before
    public void setUp() throws IOException {
        Logger.getRootLogger().setLevel(Level.WARN);
        final ParquetStoreProperties pp = new ParquetStoreProperties();
        rootDir = pp.getTempFilesDir();
        fs = FileSystem.get(new Configuration());
        fs.delete(new Path(rootDir), true);
        fs.mkdirs(new Path(rootDir));
        fs.create(new Path(rootDir + "/test.parquet"));
        fs.create(new Path(rootDir + "/test1.txt"));
        fs.create(new Path(rootDir + "/test2.parquet"));
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
    public void hasNextDoesNotSkipFiles() throws IOException {
        assertEquals(true, iterator.hasNext());
        assertEquals(fs.resolvePath(new Path(rootDir + "/test.parquet")), iterator.next());
        assertEquals(true, iterator.hasNext());
        assertEquals(fs.resolvePath(new Path(rootDir + "/test2.parquet")), iterator.next());
        assertEquals(false, iterator.hasNext());
    }

    @Test
    public void skipsNonParquetFiles() throws IOException {
        assertEquals(fs.resolvePath(new Path(rootDir + "/test.parquet")), iterator.next());
        assertEquals(fs.resolvePath(new Path(rootDir + "/test2.parquet")), iterator.next());
        assertEquals(null, iterator.next());
    }
}
