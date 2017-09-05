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
package uk.gov.gchq.gaffer.parquetstore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

/**
 * A single use implementation of the {@link ParquetStore} that will delete the data directory upon initialisation of
 * the store. This is mainly used for testing purposes.
 */
public class SingleUseParquetStore extends ParquetStore {

    private static final Logger LOGGER = LoggerFactory.getLogger(SingleUseParquetStore.class);

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        // Initialise has to be called first to ensure the properties are set.
        super.initialise(graphId, schema, properties);
        cleanUp();
        super.initialise(graphId, schema, properties);
    }

    private void cleanUp() throws StoreException {
        String dataDir = "";
        try {
            dataDir = getDataDir();
            deleteFolder(dataDir, getFS());
        } catch (final IOException e) {
            throw new StoreException("Exception deleting folder: " + dataDir, e);
        }
    }

    private void deleteFolder(final String path, final FileSystem fs) throws IOException {
        LOGGER.debug("Deleting folder {}", path);
        Path dataDir = new Path(path);
        if (fs.exists(dataDir)) {
            fs.delete(dataDir, true);
            while (fs.listStatus(dataDir.getParent()).length == 0) {
                dataDir = dataDir.getParent();
                fs.delete(dataDir, true);
            }
        }
    }
}
