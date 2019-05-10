/*
 * Copyright 2016-2019 Crown Copyright
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

public final class AccumuloPropertyNames {
    public static final String INT = "intProperty";
    public static final String STRING = "stringProperty";
    public static final String SET = "setProperty";
    public static final String DATE = "dateProperty";
    public static final String TIMESTAMP = "timestamp";
    public static final String COUNT = "count";
    public static final String VISIBILITY = "visibility";

    public static final String PROP_1 = "property1";
    public static final String PROP_2 = "property2";
    public static final String PROP_3 = "property3";
    public static final String PROP_4 = "property4";
    public static final String PROP_5 = "property5";

    public static final String TRANSIENT_1 = "transientProperty1";
    public static final String COLUMN_QUALIFIER = "columnQualifier";
    public static final String COLUMN_QUALIFIER_2 = "columnQualifier2";
    public static final String COLUMN_QUALIFIER_3 = "columnQualifier3";
    public static final String COLUMN_QUALIFIER_4 = "columnQualifier4";

    private AccumuloPropertyNames() {
        // private to prevent instantiation
    }
}
