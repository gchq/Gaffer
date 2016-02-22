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

package gaffer.accumulostore.key;

import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.file.keyfunctor.KeyFunctor;

/**
 * The AccumuloKeyPackage provides access to Factories and utility methods
 * needed for an Instance of the AccumuloStore to run, The idea of the
 * key package is to isolate all things which are dependent
 * upon any one key design, with the intent that new key
 * package can be implemented later to optimise certain queries, depending on
 * the users use case.
 */
public abstract class AccumuloKeyPackage {

    private RangeFactory rangeFactory;
    private AccumuloElementConverter keyConverter;
    private IteratorSettingFactory iteratorFactory;
    private KeyFunctor bloomFunctor;

    public RangeFactory getRangeFactory() {
        return rangeFactory;
    }

    public AccumuloElementConverter getKeyConverter() {
        return keyConverter;
    }

    public IteratorSettingFactory getIteratorFactory() {
        return iteratorFactory;
    }

    public void setRangeFactory(final RangeFactory rangeFactory) {
        this.rangeFactory = rangeFactory;
    }

    public void setKeyConverter(final AccumuloElementConverter keyConverter) {
        this.keyConverter = keyConverter;
    }

    public void setIteratorFactory(final IteratorSettingFactory iteratorFactory) {
        this.iteratorFactory = iteratorFactory;
    }

    public KeyFunctor getKeyFunctor() {
        return bloomFunctor;
    }

    public void setKeyFunctor(final KeyFunctor bloomFunctor) {
        this.bloomFunctor = bloomFunctor;
    }

    public abstract void setStoreSchema(final StoreSchema storeSchema);

    /**
     * This method is here so that users schemas can be checked for
     * compatibility against current key implementation. A
     * runtime Exception of
     * {@link gaffer.data.elementdefinition.schema.exception.SchemaException}
     * should be thrown if incompatibilities are found.
     *
     * @param storeSchema the store schema to validate
     */
    public abstract void validateSchema(final StoreSchema storeSchema);
}
