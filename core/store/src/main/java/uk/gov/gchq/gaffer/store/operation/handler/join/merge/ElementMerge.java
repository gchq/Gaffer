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
            return noMerge((Set<Map<Element, List<Element>>>) input);
        } else if (mergeType.equals(MergeType.AGAINST_KEY)) {
            return mergeAgainstKey((Set<Map<Element, List<Element>>>) input);
        } else if (mergeType.equals(MergeType.BOTH)) {
            return mergeBoth((Set<Map<Element, List<Element>>>) input);
        } else {
            throw new OperationException("A valid MergeType must be specified");
        }
    }

    private List noMerge(final Set<Map<Element, List<Element>>> input) {
        final List results = new ArrayList<>();
        if (resultsWanted.equals(ResultsWanted.KEY_ONLY)) {
            input.forEach(map -> map.forEach((keyElement, relatedList) -> results.add(keyElement)));
        } else if (resultsWanted.equals(ResultsWanted.RELATED_ONLY)) {
            input.forEach(map -> map.forEach((keyElement, relatedList) -> results.addAll(relatedList)));
        } else if (resultsWanted.equals(ResultsWanted.BOTH)) {
            input.forEach(map -> map.forEach((keyElement, relatedList) -> {
                results.add(keyElement);
                results.addAll(relatedList);
            }));
        }
        return results;
    }

    private List mergeAgainstKey(final Set<Map<Element, List<Element>>> input) {
        List<Element> results = new ArrayList<>();
        if (resultsWanted.equals(ResultsWanted.KEY_ONLY)) {
            input.forEach((map -> map.forEach((keyElement, relatedList) -> results.add(keyElement))));
        } else if (resultsWanted.equals(ResultsWanted.RELATED_ONLY)) {
            input.forEach((map -> map.forEach((keyElement, relatedList) ->
                    results.add(aggregateElement(null, relatedList, schema.getElement(keyElement.getGroup()).getIngestAggregator())))));
        } else if (resultsWanted.equals(ResultsWanted.BOTH)) {
            input.forEach((map -> map.forEach((keyElement, relatedList) -> {
                results.add(keyElement);
                results.add(aggregateElement(null, relatedList, schema.getElement(keyElement.getGroup()).getIngestAggregator()));
            })));
        }
        return results;
    }

    private List mergeBoth(final Set<Map<Element, List<Element>>> input) {
        final List<Element> results = new ArrayList<>();
        input.forEach((map -> map.forEach((keyElement, relatedList) ->
                        results.add(aggregateElement(keyElement, relatedList, schema.getElement(keyElement.getGroup()).getIngestAggregator()))
        )));
        return results;
    }

    private Element aggregateElement(final Element first, final List<Element> relatedElements, final ElementAggregator elementAggregator) {
        Element aggregatedElement = first;
        for (final Element element : relatedElements) {
            aggregatedElement = aggregatedElement != null ? aggregatedElement : element;
            if (!aggregatedElement.equals(element)) {
                aggregatedElement = elementAggregator.apply(aggregatedElement, element);
            }
        }
        return aggregatedElement;
    }
}
