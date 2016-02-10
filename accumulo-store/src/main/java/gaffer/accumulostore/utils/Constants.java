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

package gaffer.accumulostore.utils;

import org.apache.hadoop.io.Text;

public final class Constants {
    //Iterator names
    public static final String AGGREGATOR_ITERATOR_NAME = "Aggregator";
    public static final String BLOOM_FILTER_ITERATOR_NAME = "Bloom_Filter";
    public static final String ELEMENT_FILTER_ITERATOR_NAME = "Element_Filter";
    public static final String EDGE_ENTITY_DIRECTED_UNDIRECTED_FILTER_ITERATOR_NAME = "Edge_Entity_Directed_Undirected_Filter";
    public static final String QUERY_TIME_AGGREGATION_ITERATOR_NAME = "Query_Time_Aggregator";

    //Converter class to be used in iterators must be on classpath of all iterators
    public static final String ACCUMULO_ELEMENT_CONVERTER_CLASS = "accumulostore.key.element_converter";

    //Iterator options
    public static final String VIEW = "View";
    public static final String DATA_SCHEMA = "Data_Schema";
    public static final String STORE_SCHEMA = "Store_Schema";
    public static final String ENTITY_ONLY = "Entity_Only";
    public static final String ALL_EDGE_ONLY = "All_Edges_Only";
    public static final String DIRECTED_EDGE_ONLY = "Directed_Edges_Only";
    public static final String UNDIRECTED_EDGE_ONLY = "Undirected_Edges_Only";
    public static final String INCOMING_EDGE_ONLY = "Incoming_Edges_Only";
    public static final String OUTGOING_EDGE_ONLY = "Outgoing_Edges_Only";
    public static final String BLOOM_FILTER = "Bloom_Filter";
    public static final String BLOOM_FILTER_CHARSET = "ISO-8859-1";

    // Iterator priorities
    public static final int AGE_OFF_ITERATOR_PRIORITY = 10; // Applied during major compactions, minor compactions and scans.
    public static final int AGGREGATOR_ITERATOR_PRIORITY = 20; // Applied during major compactions, minor compactions and scans.
    public static final int BLOOM_FILTER_ITERATOR_PRIORITY = 31; // Applied only during scans.
    public static final int EDGE_ENTITY_DIRECTED_FILTER_ITERATOR_PRIORITY = 32; // Applied only during scans.
    public static final int ELEMENT_FILTER_ITERATOR_PRIORITY = 33; // Applied only during scans.
    public static final int QUERY_TIME_AGGREGATOR_PRIORITY = 34; // Applied only during scans.
    public static final int TRANSFORM_PRIORITY = 50; // Applied only during scans.

    //Operations options
    public static final String OPERATION_USE_ACCUMULO_PARTIONER = "accumulostore.operation.hdfs.use_accumulo_partioner";
    public static final String OPERATION_USE_PROVIDED_SPLITS = "accumulostore.operation.hdfs.use_provided_splits";
    public static final String OPERATION_AUTHORISATIONS = "accumulostore.operation.authorisations";
    public static final String OPERATION_SPLITS_FILE_PATH = "accumulostore.operation.splits_file_path";
    public static final String OPERATION_MATCH_AS_SOURCE = "accumulostore.operation.return_matched_id_as_edge_source";

    //Store factory constants
    public static final String GAFFER_UTILS_TABLE = "gafferStoreUtils";
    public static final Text DATA_SCHEMA_KEY = new Text("dataSchema");
    public static final Text STORE_SCHEMA_KEY = new Text("storeSchema");
    public static final Text KEY_PACKAGE_KEY = new Text("keyPackage");

    //General use constants
    public static final String UTF_8_CHARSET = "UTF-8";
    public static final byte[] EMPTY_BYTES = new byte[0];

    private Constants() {
        // private constructor to prevent users instantiating this class as it only contains constants.
    }
}
