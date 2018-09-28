/*
 * Copyright 2017-2018. Crown Copyright
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

import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Random;

/**
 * A single use implementation of the {@link ParquetStore}. This is mainly used for testing purposes.
 */
public class SingleUseParquetStore extends ParquetStore {

    private Random random = new Random();
    private long randomLong;

    @Override
    public void initialise(final String graphId, final Schema schema, final StoreProperties properties) throws StoreException {
        randomLong = random.nextLong();
        super.initialise(graphId, schema, properties);
    }

    @Override
    public String getDataDir() {
        return super.getDataDir() + "/" + randomLong;
    }

    @Override
    public String getTempFilesDir() {
        return super.getTempFilesDir() + "/" + randomLong;
    }
}
