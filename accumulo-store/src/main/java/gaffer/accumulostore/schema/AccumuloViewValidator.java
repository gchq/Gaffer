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

package gaffer.accumulostore.schema;

import gaffer.accumulostore.utils.StorePositions;
import gaffer.data.elementdefinition.view.View;
import gaffer.store.schema.Schema;
import gaffer.store.schema.TypeDefinition;
import gaffer.store.schema.ViewValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AccumuloViewValidator extends ViewValidator {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloViewValidator.class);

    @Override
    public boolean validate(final View view, final Schema schema) {
        boolean isValid = super.validate(view, schema);
        if (isValid && null != view && null != view.getGroupByProperties()) {
            for (String group : view.getEntityGroups()) {
                for (String groupByProperty : view.getGroupByProperties()) {
                    final TypeDefinition propTypeDef = schema.getEntity(group).getPropertyTypeDef(groupByProperty);
                    if (null != propTypeDef) {
                        final String position = propTypeDef.getPosition();
                        if (!validateGroupByPropertyPosition(groupByProperty, position)) {
                            isValid = false;
                        }
                    }
                }
            }

            for (String group : view.getEdgeGroups()) {
                for (String groupByProperty : view.getGroupByProperties()) {
                    final TypeDefinition propTypeDef = schema.getEdge(group).getPropertyTypeDef(groupByProperty);
                    if (null != propTypeDef) {
                        final String position = propTypeDef.getPosition();
                        if (!validateGroupByPropertyPosition(groupByProperty, position)) {
                            isValid = false;
                        }
                    }
                }
            }
        }

        return isValid;
    }

    protected boolean validateGroupByPropertyPosition(final String groupByProperty, final String position) {
        final boolean isValid;
        if (StorePositions.COLUMN_QUALIFIER.isEqual(position) || StorePositions.VISIBILITY.isEqual(position)) {
            isValid = true;
        } else {
            LOGGER.error("Cannot group by property " + groupByProperty + " as it is stored in position " + position + ". Only position: " + StorePositions.COLUMN_QUALIFIER.name() + " and " + StorePositions.VISIBILITY.name() + " are allowed.");
            isValid = false;
        }

        return isValid;
    }
}
