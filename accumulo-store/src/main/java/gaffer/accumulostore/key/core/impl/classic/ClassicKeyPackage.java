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
package gaffer.accumulostore.key.core.impl.classic;

import gaffer.accumulostore.key.core.AbstractCoreKeyPackage;
import gaffer.accumulostore.key.core.impl.CoreKeyBloomFunctor;
import gaffer.store.schema.StoreSchema;

public class ClassicKeyPackage extends AbstractCoreKeyPackage {
    public ClassicKeyPackage() {
        setIteratorFactory(new ClassicIteratorSettingsFactory());
        setKeyFunctor(new CoreKeyBloomFunctor());
    }

    public ClassicKeyPackage(final StoreSchema schema) {
        this();
        setStoreSchema(schema);
    }

    @Override
    public void setStoreSchema(final StoreSchema schema) {
        setRangeFactory(new ClassicRangeFactory(schema));
        setKeyConverter(new ClassicAccumuloElementConverter(schema));
    }
}
