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

package gaffer.accumulostore.key.core;

import gaffer.accumulostore.key.AccumuloKeyPackage;
import gaffer.accumulostore.utils.StorePositions;
import gaffer.data.elementdefinition.schema.exception.SchemaException;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StorePropertyDefinition;
import gaffer.store.schema.StoreSchema;

public abstract class AbstractCoreKeyPackage extends AccumuloKeyPackage {
    @Override
    public void validateSchema(final StoreSchema storeSchema) {
        for (final StoreElementDefinition storeElDef : storeSchema.getEdges().values()) {
            validateElementDef(storeElDef);
        }

        for (final StoreElementDefinition storeElDef : storeSchema.getEntities().values()) {
            validateElementDef(storeElDef);
        }
    }

    private void validateElementDef(final StoreElementDefinition storeElDef) {
        boolean seenVisibility = false;
        boolean seenTimestamp = false;
        for (final StorePropertyDefinition storePropertyDef : storeElDef.getPropertyDefinitions()) {
            if (StorePositions.VISIBILITY.isEqual(storePropertyDef.getPosition())) {
                if (!seenVisibility) {
                    seenVisibility = true;
                } else {
                    throw new SchemaException("Cannot assign position VISIBILITY on more than one property.");
                }
            } else if (StorePositions.TIMESTAMP.isEqual(storePropertyDef.getPosition())) {
                if (!seenTimestamp) {
                    seenTimestamp = true;
                } else {
                    throw new SchemaException("Cannot assign position TIMESTAMP on more than one property.");
                }
            } else if (!StorePositions.isValidName(storePropertyDef.getPosition())) {
                throw new SchemaException("Position " + storePropertyDef.getPosition() + " is not valid");
            }
        }
    }
}
