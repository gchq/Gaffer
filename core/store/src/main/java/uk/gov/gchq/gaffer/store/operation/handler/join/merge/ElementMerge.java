/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.join.merge;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.join.merge.Merge;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ElementMerge implements Merge {
    private ResultsWanted resultsWanted = ResultsWanted.BOTH;
    private MergeType mergeType = MergeType.AGAINST_KEY;
    private Schema schema;

    public ElementMerge() {
        this.resultsWanted = ResultsWanted.BOTH;
        this.mergeType = MergeType.AGAINST_KEY;
    }

    public ElementMerge(final ResultsWanted resultsWanted, final MergeType mergeType) {
        this.resultsWanted = resultsWanted;
        this.mergeType = mergeType;
    }

    public ResultsWanted getResultsWanted() {
        return resultsWanted;
    }

    public void setResultsWanted(final ResultsWanted resultsWanted) {
        this.resultsWanted = resultsWanted;
    }

    public MergeType getMergeType() {
        return mergeType;
    }

    public void setMergeType(final MergeType mergeType) {
        this.mergeType = mergeType;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(final Schema schema) {
        this.schema = schema;
    }

    @Override
    public List merge(final Set input) throws OperationException {
        if (null == schema) {
            throw new OperationException("Schema cannot be null");
        }
        if (null == mergeType) {
            throw new OperationException("A MergeType must be specified");
        }
        if (null == resultsWanted) {
            throw new OperationException("Please specify the results wanted using the ResultsWanted field");
        }

        if (mergeType.equals(MergeType.NONE)) {
            return flatten((Set<Map<Element, List<Element>>>) input);
        } else {
            return reduce((Set<Map<Element, List<Element>>>) input);
        }
    }

    private List flatten(Set<Map<Element, List<Element>>> input) {
        final List results = new ArrayList<>();
        if (resultsWanted.equals(ResultsWanted.KEY_ONLY)) {
            input.forEach(map -> map.forEach((key, value) -> results.add(key)));
        } else if (resultsWanted.equals(ResultsWanted.RELATED_ONLY)) {
            input.forEach(map -> map.forEach((key, value) -> results.add(value)));
        } else {
            input.forEach(map -> map.forEach((key, value) -> {
                results.add(key);
                results.add(value);
            }));
        }
        return results;
    }

    private List reduce(Set<Map<Element, List<Element>>> input) {
        List<Element> results = new ArrayList<>();
        for (Map<Element, List<Element>> item : input) {
            for (Map.Entry<Element, List<Element>> mapEntry : item.entrySet()) {
                Element keyElement = mapEntry.getKey();
                final SchemaElementDefinition schemaElDef = schema.getElement(keyElement.getGroup());
                final ElementAggregator agg = schemaElDef.getIngestAggregator();
                if (mergeType.equals(MergeType.AGAINST_KEY)) {
                    results.addAll(mergeAgainstKey(keyElement, mapEntry.getValue(), agg));
                } else if (mergeType.equals(MergeType.BOTH)) {
                    results.add(aggregateElement(keyElement, mapEntry.getValue(), agg));
                }
            }
        }
        return results;
    }

    private List mergeAgainstKey(Element keyElement, List<Element> relatedElements, final ElementAggregator elementAggregator) {
        List<Element> results = new ArrayList<>();

        if (resultsWanted.equals(ResultsWanted.KEY_ONLY)) {
            results.add(keyElement);
        } else if (resultsWanted.equals(ResultsWanted.RELATED_ONLY)) {
            results.add(aggregateElement(null, relatedElements, elementAggregator));
        } else if (resultsWanted.equals(ResultsWanted.BOTH)) {
            results.add(keyElement);
            results.add(aggregateElement(null, relatedElements, elementAggregator));
        }
        return results;
    }

    private Element aggregateElement(Element first, List<Element> relatedElements, final ElementAggregator elementAggregator) {
        Element aggregatedElement = first;

        for (Element element : relatedElements) {
            aggregatedElement = aggregatedElement != null ? aggregatedElement : element;
            if(!aggregatedElement.equals(element)) {
                aggregatedElement = elementAggregator.apply(aggregatedElement, element);
            }
        }
        return aggregatedElement;
    }
}