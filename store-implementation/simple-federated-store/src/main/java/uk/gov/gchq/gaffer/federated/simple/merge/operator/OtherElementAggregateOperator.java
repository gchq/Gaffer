/*
 * Copyright 2025 Crown Copyright
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

package uk.gov.gchq.gaffer.federated.simple.merge.operator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.collections4.IterableUtils;

import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

public class OtherElementAggregateOperator extends ElementAggregateOperator {

    @Override
    public Iterable<Element> apply(Iterable<Element> update, Iterable<Element> state) {
        List<Element> stateList = IterableUtils.toList(state);
        List<Element> updateList = IterableUtils.toList(update);

        List<Element> merged = new ArrayList<>();
        for (Element element : stateList) {
            // Set up the aggregator for this group based on the schema
            ElementAggregator aggregator = new ElementAggregator();
            boolean shouldMerge = false;
            if (schema != null) {
                SchemaElementDefinition elementDefinition = schema.getElement(element.getGroup());
                aggregator = elementDefinition.getIngestAggregator();
                shouldMerge = elementDefinition.isAggregate();
            }

            if (!shouldMerge) {
                merged.add(element);
            }

            boolean hasMerged = false;
            Collection<Element> deleted = new ArrayList<>();
            for (Element otherElement : updateList) {
                if (!element.getGroup().equals(otherElement.getGroup())) {
                    continue;
                }

                if (element.equals(otherElement)) {
                    deleted.add(otherElement);
                }

                if ((element instanceof Entity)
                        && (otherElement instanceof Entity)
                        && ((Entity) element).getVertex().equals(((Entity) otherElement).getVertex())) {
                    Element result = aggregator.apply(otherElement.shallowClone(), element).shallowClone();
                    merged.add(result);
                    hasMerged = true;
                    deleted.add(otherElement);
                }

                if ((element instanceof Edge)
                        && (otherElement instanceof Edge)
                        && ((Edge) element).getSource().equals(((Edge) otherElement).getSource())
                        && ((Edge) element).getDestination().equals(((Edge) otherElement).getDestination())
                        && ((Edge) element).getDirectedType().equals(((Edge) otherElement).getDirectedType())) {
                    Element result = aggregator.apply(otherElement.shallowClone(), element);
                    merged.add(result);
                    hasMerged = true;
                    deleted.add(otherElement);
                }
            }

            updateList.removeAll(deleted);

            if (!hasMerged) {
                merged.add(element);
            }
        }

        // Add anything left in the other List
        // These must be elements that aren't in the first list
        merged.addAll(updateList);
        return merged;
    }

}
