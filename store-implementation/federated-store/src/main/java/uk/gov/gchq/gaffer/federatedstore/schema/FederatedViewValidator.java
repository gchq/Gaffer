/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.schema;

import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;

public class FederatedViewValidator extends ViewValidator {

    @Override
    public ValidationResult validate(final View view, final Schema schema, final Set<StoreTrait> storeTraits) {
        final ValidationResult rtn = new ValidationResult();

        final boolean isStoreOrdered = storeTraits.contains(StoreTrait.ORDERED);

        if (null != view) {
            final ValidationResult entitiesResult = getEntityResult(view, schema, storeTraits, isStoreOrdered);
            final ValidationResult edgeResult = getEdgeResult(view, schema, storeTraits, isStoreOrdered);

            final boolean isEntityViewInvalid = !entitiesResult.isValid();
            final boolean isEdgeViewInvalid = !edgeResult.isValid();
            final boolean isEntityViewInvalidAndTheOnlyViewRequested = isEntityViewInvalid && (isNull(view.getEdges()) || view.getEdges().isEmpty());
            final boolean isEdgeViewInvalidAndTheOnlyViewRequested = isEdgeViewInvalid && (isNull(view.getEntities()) || view.getEntities().isEmpty());

            if (isEntityViewInvalid && isEdgeViewInvalid) {
                rtn.add(entitiesResult);
                rtn.add(edgeResult);
            } else if (isEntityViewInvalidAndTheOnlyViewRequested) {
                rtn.add(entitiesResult);
            } else if (isEdgeViewInvalidAndTheOnlyViewRequested) {
                rtn.add(edgeResult);
            }
        }
        return rtn;
    }

    protected ValidationResult getEdgeResult(final View view, final Schema schema, final Set<StoreTrait> storeTraits, final boolean isStoreOrdered) {
        final Function<Map.Entry<String, ViewElementDefinition>, ValidationResult> validationResultFunction = e -> {
            final ValidationResult singleResult = new ValidationResult();
            validateEdge(new View.Builder().edge(e.getKey(), e.getValue()).build(), schema, storeTraits, isStoreOrdered, singleResult);
            return singleResult;
        };

        return getElementResult(validationResultFunction, view.getEdges());
    }

    protected ValidationResult getEntityResult(final View view, final Schema schema, final Set<StoreTrait> storeTraits, final boolean isStoreOrdered) {

        final Function<Map.Entry<String, ViewElementDefinition>, ValidationResult> validationResultFunction = e -> {
            final ValidationResult singleResult = new ValidationResult();
            validateEntities(new View.Builder().entity(e.getKey(), e.getValue()).build(), schema, storeTraits, isStoreOrdered, singleResult);
            return singleResult;
        };
        return getElementResult(validationResultFunction, view.getEntities());
    }

    private ValidationResult getElementResult(final Function<Map.Entry<String, ViewElementDefinition>, ValidationResult> validationResultFunction, final Map<String, ViewElementDefinition> elementDefinitionMap) {
        final ValidationResult rtn = new ValidationResult();

        final List<ValidationResult> viewResults = elementDefinitionMap.entrySet().stream()
                .map(validationResultFunction)
                .collect(Collectors.toList());

        final boolean isAnyViewsValid = viewResults.stream().anyMatch(ValidationResult::isValid);

        if (!isAnyViewsValid) {
            viewResults.forEach(rtn::add);
        }

        return rtn;
    }
}
