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

package uk.gov.gchq.gaffer.accumulostore.utils;

public final class AccumuloStoreConstants {
    //Iterator names
    public static final String VALIDATOR_ITERATOR_NAME = "Validator";
    public static final String AGGREGATOR_ITERATOR_NAME = "Aggregator";
    public static final String BLOOM_FILTER_ITERATOR_NAME = "Bloom_Filter";
    public static final String ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_NAME = "Element_Pre_Aggregation_Filter";
    public static final String ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_NAME = "Element_Post_Aggregation_Filter";

    public static final String EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_NAME = "Edge_Entity_Directed_Undirected_Incoming_Outgoing_Filter";
    public static final String COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_NAME = "Column_Qualifier_Aggregator";
    public static final String ROW_ID_AGGREGATOR_ITERATOR_NAME = "Row_ID_Aggregator";
    public static final String RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_NAME = "Range_Element_Property_Filter";

    // Converter class to be used in iterators must be on classpath of all
    // iterators
    public static final String ACCUMULO_ELEMENT_CONVERTER_CLASS = "accumulostore.key.element_converter";

    // Iterator options
    public static final String VIEW = "View";
    public static final String SCHEMA = "Schema";
    public static final String INCLUDE_ENTITIES = "Include_All_Entities";
    public static final String INCLUDE_EDGES = "Include_All_Edges";
    public static final String DIRECTED_EDGE_ONLY = "Directed_Edges_Only";
    public static final String UNDIRECTED_EDGE_ONLY = "Undirected_Edges_Only";
    public static final String INCOMING_EDGE_ONLY = "Incoming_Edges_Only";
    public static final String OUTGOING_EDGE_ONLY = "Outgoing_Edges_Only";
    public static final String DEDUPLICATE_UNDIRECTED_EDGES = "Deduplicate_Undirected_Edges";
    public static final String BLOOM_FILTER = "Bloom_Filter";
    public static final String BLOOM_FILTER_CHARSET = "ISO-8859-1";
    public static final String COLUMN_FAMILY = "columnFamily";

    // Iterator priorities
    // Applied during major compactions, minor compactions  and scans.
    public static final int AGGREGATOR_ITERATOR_PRIORITY = 10;
    // Applied during major compactions, minor compactions and scans.
    public static final int VALIDATOR_ITERATOR_PRIORITY = 20;
    // Applied only during scans.
    public static final int BLOOM_FILTER_ITERATOR_PRIORITY = 31;
    // Applied only during scans.
    public static final int RANGE_ELEMENT_PROPERTY_FILTER_ITERATOR_PRIORITY = 32;
    // Applied only during scans.
    public static final int EDGE_ENTITY_DIRECTED_UNDIRECTED_INCOMING_OUTGOING_FILTER_ITERATOR_PRIORITY = 33;
    // Applied only during scans.
    public static final int ELEMENT_PRE_AGGREGATION_FILTER_ITERATOR_PRIORITY = 34;
    // Applied only during scans.
    public static final int ROW_ID_AGGREGATOR_ITERATOR_PRIORITY = 35;
    // Applied only during scans.
    public static final int COLUMN_QUALIFIER_AGGREGATOR_ITERATOR_PRIORITY = 36;
    // Applied only during scans.
    public static final int ELEMENT_POST_AGGREGATION_FILTER_ITERATOR_PRIORITY = 37;

    // Operations options
    public static final String ADD_ELEMENTS_FROM_HDFS_SKIP_IMPORT = "accumulostore.operation.hdfs.skip_import";

    // General use constants
    public static final byte[] EMPTY_BYTES = new byte[0];

    private AccumuloStoreConstants() {
        // private constructor to prevent users instantiating this class as it
        // only contains constants.
    }
}
