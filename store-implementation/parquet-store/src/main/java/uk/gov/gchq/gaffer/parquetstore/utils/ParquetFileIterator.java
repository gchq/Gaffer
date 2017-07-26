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

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ParquetFileIterator implements Iterator<Path> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetFileIterator.class);
    private final List<Path> files;
    private final FileSystem fs;
    private int fileIndex;

    public ParquetFileIterator(final Path rootDir, final FileSystem fs) throws IOException {
        this.fs = fs;
        this.files = new ArrayList<>();
        getFiles(rootDir);
        this.fileIndex = -1;
        LOGGER.debug("Generated a ParquetFileIterator with " + this.files.size() + " files");
    }

    private void getFiles(final Path path) throws IOException {
        if (fs.isFile(path)) {
            if (path.getName().endsWith(".parquet")) {
                files.add(path);
            }
        } else {
            for (final FileStatus file: fs.listStatus(path)) {
                getFiles(file.getPath());
            }
        }
    }

    @Override
    public boolean hasNext() {
        return fileIndex < files.size() - 1;
    }

    @Override
    public Path next() {
        if (hasNext()) {
            fileIndex = fileIndex + 1;
            return files.get(fileIndex);
        }
        return null;
    }
}
