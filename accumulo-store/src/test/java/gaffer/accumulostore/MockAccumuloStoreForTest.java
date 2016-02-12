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

package gaffer.accumulostore;

import gaffer.accumulostore.key.core.AbstractCoreKeyPackage;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityKeyPackage;
import gaffer.commonutil.PathUtil;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.StoreException;
import gaffer.store.schema.StoreSchema;

public class MockAccumuloStoreForTest extends MockAccumuloStore {
    public MockAccumuloStoreForTest() {
        this(ByteEntityKeyPackage.class);
    }

    public MockAccumuloStoreForTest(final Class<? extends AbstractCoreKeyPackage> keyPackageClass) {
        final DataSchema dataSchema = DataSchema.fromJson(PathUtil.dataSchema(getClass()));
        final StoreSchema storeSchema = StoreSchema.fromJson(PathUtil.storeSchema(getClass()));
        final AccumuloProperties properties = new AccumuloProperties(PathUtil.storeProps(getClass()));
        properties.setKeyPackageClass(keyPackageClass.getName());

        try {
            initialise(dataSchema, storeSchema, properties);
        } catch (StoreException e) {
            throw new RuntimeException(e);
        }
    }
}
