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
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.store.schema.SchemaElementDefinition;
import gaffer.store.schema.Schema;
import gaffer.store.schema.TypeDefinition;

public abstract class AbstractCoreKeyPackage extends AccumuloKeyPackage {
    @Override
    public void validateSchema(final Schema schema) {
        for (final SchemaElementDefinition storeElDef : schema.getEdges().values()) {
            validateElementDef(storeElDef);
        }

        for (final SchemaElementDefinition storeElDef : schema.getEntities().values()) {
            validateElementDef(storeElDef);
        }
    }

    private void validateElementDef(final SchemaElementDefinition storeElDef) {
        boolean seenVisibility = false;
        boolean seenTimestamp = false;
        for (final TypeDefinition storePropertyDef : storeElDef.getPropertyTypeDefs()) {
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
