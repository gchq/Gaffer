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

package uk.gov.gchq.gaffer.graph.hook.migrate;

import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;

import java.util.HashMap;
import java.util.Map;

/**
 * POJO to hold all relevant ElementFilters and ElementTransformers with the related ViewElementDefinition.
 */
public class ViewWithTransformationsAndFilters {
    private ViewElementDefinition viewElementDefinition;
    private Map<String, ElementFilter> entitiesPostAggregationFilterMap;
    private Map<String, ElementFilter> edgesPostAggregationFilterMap;
    private Map<String, ElementTransformer> entitiesTransformerMap;
    private Map<String, ElementTransformer> edgesTransformerMap;
    private Map<String, ElementFilter> entitiesPostTransformFilterMap;
    private Map<String, ElementFilter> edgesPostTransformFilterMap;

    ViewWithTransformationsAndFilters() {
        viewElementDefinition = new ViewElementDefinition();
        entitiesPostAggregationFilterMap = new HashMap<>();
        edgesPostAggregationFilterMap = new HashMap<>();
        entitiesTransformerMap = new HashMap<>();
        edgesTransformerMap = new HashMap<>();
        entitiesPostTransformFilterMap = new HashMap<>();
        edgesPostTransformFilterMap = new HashMap<>();
    }

    public ViewElementDefinition getViewElementDefinition() {
        return viewElementDefinition;
    }

    public void setViewElementDefinition(final ViewElementDefinition viewElementDefinition) {
        this.viewElementDefinition = viewElementDefinition;
    }

    public Map<String, ElementFilter> getEntitiesPostAggregationFilterMap() {
        return entitiesPostAggregationFilterMap;
    }

    public void setEntitiesPostAggregationFilterMap(final Map<String, ElementFilter> entitiesPostAggregationFilterMap) {
        if (this.entitiesPostAggregationFilterMap.isEmpty()) {
            this.entitiesPostAggregationFilterMap = entitiesPostAggregationFilterMap;
        } else {
            this.entitiesPostAggregationFilterMap.putAll(entitiesPostAggregationFilterMap);
        }
    }

    public Map<String, ElementFilter> getEdgesPostAggregationFilterMap() {
        return edgesPostAggregationFilterMap;
    }

    public void setEdgesPostAggregationFilterMap(final Map<String, ElementFilter> edgesPostAggregationFilterMap) {
        if (edgesPostAggregationFilterMap.isEmpty()) {
            this.edgesPostAggregationFilterMap = edgesPostAggregationFilterMap;
        } else {
            this.edgesPostAggregationFilterMap.putAll(edgesPostAggregationFilterMap);
        }
    }

    public Map<String, ElementTransformer> getEntitiesTransformerMap() {
        return entitiesTransformerMap;
    }

    public void setEntitiesTransformerMap(final Map<String, ElementTransformer> entitiesTransformerMap) {
        if (this.entitiesTransformerMap.isEmpty()) {
            this.entitiesTransformerMap = entitiesTransformerMap;
        } else {
            this.entitiesTransformerMap.putAll(entitiesTransformerMap);
        }
    }

    public Map<String, ElementTransformer> getEdgesTransformerMap() {
        return edgesTransformerMap;
    }

    public void setEdgesTransformerMap(final Map<String, ElementTransformer> edgesTransformerMap) {
        if (this.edgesTransformerMap.isEmpty()) {
            this.edgesTransformerMap = edgesTransformerMap;
        } else {
            this.edgesTransformerMap.putAll(edgesTransformerMap);
        }
    }

    public Map<String, ElementFilter> getEntitiesPostTransformFilterMap() {
        return entitiesPostTransformFilterMap;
    }

    public void setEntitiesPostTransformFilterMap(final Map<String, ElementFilter> entitiesPostTransformFilterMap) {
        if (this.entitiesPostTransformFilterMap.isEmpty()) {
            this.entitiesPostTransformFilterMap = entitiesPostTransformFilterMap;
        } else {
            this.entitiesPostTransformFilterMap.putAll(entitiesPostTransformFilterMap);
        }
    }

    public Map<String, ElementFilter> getEdgesPostTransformFilterMap() {
        return edgesPostTransformFilterMap;
    }

    public void setEdgesPostTransformFilterMap(final Map<String, ElementFilter> edgesPostTransformFilterMap) {
        if (this.edgesPostTransformFilterMap.isEmpty()) {
            this.edgesPostTransformFilterMap = edgesPostTransformFilterMap;
        } else {
            this.edgesPostTransformFilterMap.putAll(edgesPostTransformFilterMap);
        }
    }

    public static ViewWithTransformationsAndFilters getTransformationsAndFiltersFromElements(final Map<String, ViewWithTransformationsAndFilters> migratedEdges, final Map<String, ViewWithTransformationsAndFilters> migratedEntities) {
        final ViewWithTransformationsAndFilters transformationsAndFilters = new ViewWithTransformationsAndFilters();
        for (final Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEntities.entrySet()) {
            transformationsAndFilters.setEdgesPostAggregationFilterMap(entry.getValue().getEdgesPostAggregationFilterMap());
            transformationsAndFilters.setEntitiesPostAggregationFilterMap(entry.getValue().getEntitiesPostAggregationFilterMap());
            transformationsAndFilters.setEdgesPostTransformFilterMap(entry.getValue().getEdgesPostTransformFilterMap());
            transformationsAndFilters.setEntitiesPostTransformFilterMap(entry.getValue().getEntitiesPostTransformFilterMap());
            transformationsAndFilters.setEdgesTransformerMap(entry.getValue().getEdgesTransformerMap());
            transformationsAndFilters.setEntitiesTransformerMap(entry.getValue().getEntitiesTransformerMap());
        }
        for (final Map.Entry<String, ViewWithTransformationsAndFilters> entry : migratedEdges.entrySet()) {
            transformationsAndFilters.setEdgesPostAggregationFilterMap(entry.getValue().getEdgesPostAggregationFilterMap());
            transformationsAndFilters.setEntitiesPostAggregationFilterMap(entry.getValue().getEntitiesPostAggregationFilterMap());
            transformationsAndFilters.setEdgesPostTransformFilterMap(entry.getValue().getEdgesPostTransformFilterMap());
            transformationsAndFilters.setEntitiesPostTransformFilterMap(entry.getValue().getEntitiesPostTransformFilterMap());
            transformationsAndFilters.setEdgesTransformerMap(entry.getValue().getEdgesTransformerMap());
            transformationsAndFilters.setEntitiesTransformerMap(entry.getValue().getEntitiesTransformerMap());
        }
        return transformationsAndFilters;
    }
}
