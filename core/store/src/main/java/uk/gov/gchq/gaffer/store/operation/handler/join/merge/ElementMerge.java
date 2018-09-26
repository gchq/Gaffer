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
    private ResultsWanted resultsWanted;
    private MergeType mergeType;
    private Schema schema;

    public ElementMerge() {
        this.resultsWanted = ResultsWanted.BOTH;
        this.mergeType = MergeType.AGAINST_KEY;
        this.schema = null;
    }

    public ElementMerge(final ResultsWanted resultsWanted, final MergeType mergeType) {
        this.resultsWanted = resultsWanted;
        this.mergeType = mergeType;
        this.schema = schema;
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
        if (mergeType.equals(MergeType.NONE)) {
            return flatten(input);
        } else if (mergeType.equals(MergeType.AGAINST_KEY)) {
            return reduceAgainstKey(input);
        } else if (mergeType.equals(MergeType.BOTH)) {
            return reduceAll(input);
        } else {
            return new ArrayList<>(input);
        }
    }

    private List flatten(Set input) {
        // Flatten, [E1]:[E2,E3] -> [E1, E2, E3]
        List results = new ArrayList<>();
        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) input) {
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                Element lhsElement = (Element) mapEntry.getKey();
                if (resultsWanted.equals(ResultsWanted.KEY_ONLY) || resultsWanted.equals(ResultsWanted.BOTH)) {
                    results.add(lhsElement);
                }
                for (Object rhsObject : mapEntry.getValue()) {
                    Element rhsElement = (Element) rhsObject;
                    if (resultsWanted.equals(ResultsWanted.RELATED_ONLY) || resultsWanted.equals(ResultsWanted.BOTH)) {
                        results.add(rhsElement);
                    }
                }
            }
        }
        return results;
    }

    private List reduceAgainstKey(Set input) {
        // Wanting both, reducing right, [E1]:[E1,E2] -> [E1, E3]
        List results = new ArrayList<>();

        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) input) {
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                Element lhsElement = (Element) mapEntry.getKey();
                final SchemaElementDefinition schemaElDef = schema.getElement(lhsElement.getGroup());
                final ElementAggregator agg = schemaElDef.getIngestAggregator();
                Element aggregatedElement = null;
                for (Object rhsObject : mapEntry.getValue()) {
                    if (null != aggregatedElement) {
                        aggregatedElement = agg.apply(aggregatedElement, (Element) rhsObject);
                    } else {
                        aggregatedElement = (Element) rhsObject;
                    }
                }
                if (resultsWanted.equals(ResultsWanted.KEY_ONLY)) {
                    results.add(lhsElement);
                } else if (resultsWanted.equals(ResultsWanted.RELATED_ONLY)) {
                    results.add(aggregatedElement);
                } else if (resultsWanted.equals(ResultsWanted.BOTH)) {
                    results.add(lhsElement);
                    results.add(aggregatedElement);
                }
            }
        }

        return results;
    }

    private List<Element> reduceAll(Set input) {
        // Wanting both, reducing both, [E1]:[E1,E2] -> [E4]
        List<Element> results = new ArrayList<>();
        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) input) {
            Element aggregatedElement = null;
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                Element lhsElement = (Element) mapEntry.getKey();
                final SchemaElementDefinition schemaElDef = schema.getElement(lhsElement.getGroup());
                final ElementAggregator agg = schemaElDef.getIngestAggregator();
                if (null == aggregatedElement) {
                    aggregatedElement = lhsElement;
                }
                for (Object rhsObject : mapEntry.getValue()) {
                    aggregatedElement = agg.apply(aggregatedElement, (Element) rhsObject);
                }
                results.add(aggregatedElement);
            }
        }
        return results;
    }
}