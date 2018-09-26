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

package uk.gov.gchq.gaffer.operation.util.merge;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.function.ElementAggregator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ElementMerge implements Merge {
    private ResultsWanted resultsWanted;
    private ReduceType reduceType;
    private ElementAggregator reduceAggregator;

    public ElementMerge() {
        this.resultsWanted = ResultsWanted.BOTH;
        this.reduceType = ReduceType.AGAINST_KEY;
        this.reduceAggregator = null;
    }

    public ElementMerge(final ResultsWanted resultsWanted, final ReduceType reduceType, final ElementAggregator reduceAggregator) {
        this.resultsWanted = resultsWanted;
        this.reduceType = reduceType;
        this.reduceAggregator = reduceAggregator;
    }

    public ResultsWanted getResultsWanted() {
        return resultsWanted;
    }

    public void setResultsWanted(ResultsWanted resultsWanted) {
        this.resultsWanted = resultsWanted;
    }

    public ReduceType getReduceType() {
        return reduceType;
    }

    public void setReduceType(ReduceType reduceType) {
        this.reduceType = reduceType;
    }

    public ElementAggregator getReduceFunction() {
        return reduceAggregator;
    }

    public void setReduceFunction(ElementAggregator reduceAggregator) {
        this.reduceAggregator = reduceAggregator;
    }

    @Override
    public Iterable merge(final Iterable input) {
        if (reduceAggregator == null || reduceType.equals(ReduceType.NONE)) {
            return flatten(input);
        } else if (null != reduceAggregator) {
            if (reduceType.equals(ReduceType.AGAINST_KEY)) {
                return reduceAgainstKey(input);
            } else if (reduceType.equals(ReduceType.BOTH)) {
                return reduceAll(input);
            } else {
                return input;
            }
        } else {
            return input;
        }
    }

    private List flatten(Iterable input) {
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

    private List reduceAgainstKey(Iterable input) {
        // Wanting both, reducing right, [E1]:[E1,E2] -> [E1, E3]
        List results = new ArrayList<>();
        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) input) {
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                Element lhsElement = (Element) mapEntry.getKey();
                Element aggregatedElement = null;
                for (Object rhsObject : mapEntry.getValue()) {
                    if (null != aggregatedElement) {
                        aggregatedElement = reduceAggregator.apply(aggregatedElement, (Element) rhsObject);
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

    private List<Element> reduceAll(Iterable input) {
        // Wanting both, reducing both, [E1]:[E1,E2] -> [E4]
        List<Element> results = new ArrayList<>();
        for (Map<Object, List<Object>> item : (Iterable<? extends Map>) input) {
            Element aggregatedElement = null;
            for (Map.Entry<Object, List<Object>> mapEntry : item.entrySet()) {
                Element lhsElement = (Element) mapEntry.getKey();
                if (null == aggregatedElement) {
                    aggregatedElement = lhsElement;
                }
                for (Object rhsObject : mapEntry.getValue()) {
                    aggregatedElement = reduceAggregator.apply(aggregatedElement, (Element) rhsObject);
                }
                results.add(aggregatedElement);
            }
        }
        return results;
    }
}