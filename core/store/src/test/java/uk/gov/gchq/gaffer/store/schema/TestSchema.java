/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.store.schema;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.store.TestTypes;

import java.util.HashMap;
import java.util.Map;

import static uk.gov.gchq.gaffer.store.TestTypes.BOOLEAN_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.DATE_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.FREQMAP_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.INTEGER_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.LONG_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.SET_STRING_TYPE;
import static uk.gov.gchq.gaffer.store.TestTypes.STRING_TYPE;

/**
 * Static utility class for creating {@link Schema} objects for use in test classes.
 */
public enum TestSchema {

    EMPTY_SCHEMA(new Builder().emptySchema().build()),
    BASIC_SCHEMA(new Builder().basicSchema().build()),
    VISIBILITY_SCHEMA(new Builder().visibilitySchema().build()),
    AGGREGATION_SCHEMA(new Builder().aggregationSchema().build()),
    FULL_SCHEMA(new Builder().fullSchema().build());

    private final Schema schema;

    TestSchema(final Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public String toString() {
        return WordUtils.capitalize(StringUtils.lowerCase(name().replaceAll("_", " ")));
    }

    private static class Builder {

        private Schema.Builder schemaBuilder;

        private Map<String, String> defaultElementProperties = new HashMap<>();

        Builder() {
            this.schemaBuilder = new Schema.Builder();

            defaultElementProperties.put(TestPropertyNames.COUNT, TestTypes.PROP_COUNT);
            defaultElementProperties.put(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER);
            defaultElementProperties.put(TestPropertyNames.PROP_2, TestTypes.PROP_LONG);
            defaultElementProperties.put(TestPropertyNames.PROP_3, TestTypes.PROP_STRING);
            defaultElementProperties.put(TestPropertyNames.PROP_4, TestTypes.PROP_MAP);
            defaultElementProperties.put(TestPropertyNames.PROP_5, TestTypes.PROP_SET_STRING);
            defaultElementProperties.put(TestPropertyNames.DATE, TestTypes.PROP_DATE);
            defaultElementProperties.put(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP);
            defaultElementProperties.put(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY);
        }

        public Builder emptySchema() {
            // Empty placeholder method
            return this;
        }

        public Builder basicSchema() {
            schemaBuilder.type(TestTypes.VERTEX_STRING, STRING_TYPE)
                    .type(TestTypes.DIRECTED_EITHER, BOOLEAN_TYPE)
                    .type(TestTypes.PROP_COUNT, LONG_TYPE)
                    .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .build())
                    .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .build());
            return this;
        }

        public Builder visibilitySchema() {
            schemaBuilder.type(TestTypes.VERTEX_STRING, STRING_TYPE)
                    .type(TestTypes.DIRECTED_EITHER, BOOLEAN_TYPE)
                    .type(TestTypes.PROP_COUNT, LONG_TYPE)
                    .type(TestTypes.VISIBILITY, STRING_TYPE)
                    .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .build())
                    .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .build())
                    .visibilityProperty(TestPropertyNames.VISIBILITY);
            return this;
        }

        public Builder fullSchema() {
            schemaBuilder.type(TestTypes.VERTEX_STRING, STRING_TYPE)
                    .type(TestTypes.DIRECTED_EITHER, BOOLEAN_TYPE)
                    .type(TestTypes.PROP_COUNT, LONG_TYPE)
                    .type(TestTypes.PROP_INTEGER, INTEGER_TYPE)
                    .type(TestTypes.PROP_LONG, LONG_TYPE)
                    .type(TestTypes.PROP_STRING, STRING_TYPE)
                    .type(TestTypes.PROP_MAP, FREQMAP_TYPE)
                    .type(TestTypes.PROP_SET_STRING, SET_STRING_TYPE)
                    .type(TestTypes.PROP_DATE, DATE_TYPE)
                    .type(TestTypes.TIMESTAMP, LONG_TYPE)
                    .type(TestTypes.VISIBILITY, STRING_TYPE)
                    .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.PROP_4, TestTypes.PROP_MAP)
                            .property(TestPropertyNames.PROP_5, TestTypes.PROP_SET_STRING)
                            .property(TestPropertyNames.DATE, TestTypes.PROP_DATE)
                            .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .groupBy(TestPropertyNames.PROP_3)
                            .aggregate(true)
                            .build())
                    .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.PROP_4, TestTypes.PROP_MAP)
                            .property(TestPropertyNames.PROP_5, TestTypes.PROP_SET_STRING)
                            .property(TestPropertyNames.DATE, TestTypes.PROP_DATE)
                            .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .aggregate(false)
                            .build())
                    .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.PROP_4, TestTypes.PROP_MAP)
                            .property(TestPropertyNames.PROP_5, TestTypes.PROP_SET_STRING)
                            .property(TestPropertyNames.DATE, TestTypes.PROP_DATE)
                            .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .groupBy(TestPropertyNames.PROP_3)
                            .aggregate(true)
                            .build())
                    .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.PROP_4, TestTypes.PROP_MAP)
                            .property(TestPropertyNames.PROP_5, TestTypes.PROP_SET_STRING)
                            .property(TestPropertyNames.DATE, TestTypes.PROP_DATE)
                            .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .aggregate(false)
                            .build())
                    .visibilityProperty(TestPropertyNames.VISIBILITY);
            return this;
        }

        public Builder aggregationSchema() {
            schemaBuilder.type(TestTypes.VERTEX_STRING, STRING_TYPE)
                    .type(TestTypes.DIRECTED_EITHER, BOOLEAN_TYPE)
                    .type(TestTypes.PROP_COUNT, LONG_TYPE)
                    .type(TestTypes.PROP_INTEGER, INTEGER_TYPE)
                    .type(TestTypes.PROP_LONG, LONG_TYPE)
                    .type(TestTypes.PROP_STRING, STRING_TYPE)
                    .type(TestTypes.VISIBILITY, STRING_TYPE)
                    .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .groupBy(TestPropertyNames.PROP_3)
                            .aggregate(true)
                            .build())
                    .entity(TestGroups.ENTITY_2, new SchemaEntityDefinition.Builder()
                            .vertex(TestTypes.VERTEX_STRING)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .aggregate(false)
                            .build())
                    .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .groupBy(TestPropertyNames.PROP_3)
                            .aggregate(true)
                            .build())
                    .edge(TestGroups.EDGE_2, new SchemaEdgeDefinition.Builder()
                            .source(TestTypes.VERTEX_STRING)
                            .destination(TestTypes.VERTEX_STRING)
                            .directed(TestTypes.DIRECTED_EITHER)
                            .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                            .property(TestPropertyNames.PROP_1, TestTypes.PROP_INTEGER)
                            .property(TestPropertyNames.PROP_2, TestTypes.PROP_LONG)
                            .property(TestPropertyNames.PROP_3, TestTypes.PROP_STRING)
                            .property(TestPropertyNames.VISIBILITY, TestTypes.VISIBILITY)
                            .aggregate(false)
                            .build())
                    .visibilityProperty(TestPropertyNames.VISIBILITY);
            return this;
        }

        public Builder withFullEntity(final String group) {
            schemaBuilder.entity(group, new SchemaEntityDefinition.Builder()
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .build());
            return this;
        }

        public Builder withSimpleEntity(final String group) {
            schemaBuilder.entity(group, new SchemaEntityDefinition.Builder()
                    .properties(defaultElementProperties)
                    .build());
            return this;
        }

        public Builder withFullEdge(final String group) {
            schemaBuilder.edge(group, new SchemaEdgeDefinition.Builder()
                    .properties(defaultElementProperties)
                    .build());
            return this;
        }

        public Builder withSimpleEdge(final String group) {
            schemaBuilder.edge(group, new SchemaEdgeDefinition.Builder()
                    .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                    .build());
            return this;
        }

        public Builder withVisibilityProperty() {
            schemaBuilder.visibilityProperty(TestPropertyNames.VISIBILITY);
            return this;
        }

        public Builder withTimestampProperty() {
            schemaBuilder.timestampProperty(TestPropertyNames.TIMESTAMP);
            return this;
        }

        public Schema build() {
            return schemaBuilder.build();
        }
    }

}
