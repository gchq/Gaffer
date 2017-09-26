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

package uk.gov.gchq.gaffer.store;

/**
 * A {@code StoreTrait} defines functionality for {@link uk.gov.gchq.gaffer.store.Store} implementations.
 */
public enum StoreTrait {
    /**
     * Similar {@link uk.gov.gchq.gaffer.data.element.Element}s are aggregated/merged together based on the groupBy logic in the schema at ingest.
     */
    INGEST_AGGREGATION,

    /**
     * Similar {@link uk.gov.gchq.gaffer.data.element.Element}s are aggregated/merged together based on the groupBy logic in the view.
     */
    QUERY_AGGREGATION,

    /**
     * Most stores should have this trait if they deal with Aggregation as if you use Operation.validatePreAggregationFilter(Element) in you handlers,
     * it will deal with the filtering for you.
     * {@link uk.gov.gchq.gaffer.data.element.Element}s are filtered using {@link java.util.function.Predicate}s defined in a
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     */
    PRE_AGGREGATION_FILTERING,

    /**
     * Most stores should have this trait if they deal with Aggregation as if you use Operation.validatePostFilter(Element) in you handlers,
     * it will deal with the filtering for you.
     * {@link uk.gov.gchq.gaffer.data.element.Element}s are filtered using {@link java.util.function.Predicate}s defined in a
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     */
    POST_AGGREGATION_FILTERING,

    /**
     * Most stores should have this trait if they support Transformations as if you use Operation.validatePostTransformationFilter(Element) in you handlers,
     * it will deal with the filtering for you.
     * {@link uk.gov.gchq.gaffer.data.element.Element}s are filtered using {@link java.util.function.Predicate}s defined in a
     * {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     */
    POST_TRANSFORMATION_FILTERING,

    /**
     * {@link uk.gov.gchq.gaffer.data.element.Element} {@link uk.gov.gchq.gaffer.data.element.Properties} are transformed using
     * {@link java.util.function.Function}s defined in a {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View}.
     */
    TRANSFORMATION,

    /**
     * Elements will be validated continuously and removed if they are found to
     * be invalid based on {@link java.util.function.Predicate}s defined in the
     * {@link uk.gov.gchq.gaffer.store.schema.Schema}.
     */
    STORE_VALIDATION,

    /**
     * Ordered stores keep their elements ordered to optimise lookups. An example
     * of an ordered store is Accumulo, which orders the element keys.
     * Stores that are ordered have special characteristics such as requiring
     * serialisers that preserve ordering of the keyed properties.
     */
    ORDERED,

    /**
     * Stores with this trait support user-defined visibility settings to prevent authorised access to records
     * which a user does not have permissions to see.
     */
    VISIBILITY
}
