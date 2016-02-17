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
package gaffer.accumulostore.key.core.impl.gaffer1;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.AbstractAccumuloElementConverterTest;
import gaffer.accumulostore.key.core.impl.classic.ClassicAccumuloElementConverter;
import gaffer.store.schema.StoreSchema;

/**
 * Tests are inherited from AbstractAccumuloElementConverterTest.
 */
public class Gaffer1AccumuloElementConverterTest extends AbstractAccumuloElementConverterTest {

    @Override
    protected AccumuloElementConverter createConverter(final StoreSchema storeSchema) {
        return new ClassicAccumuloElementConverter(storeSchema);
    }
}