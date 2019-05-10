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

package uk.gov.gchq.gaffer.commonutil;

/**
 * Test schema types for use in Gaffer test classes.
 *
 * @deprecated Please use the equivalent TestTypes class in the store module
 */
@Deprecated
public final class TestTypes {

    private TestTypes() {
        //Private to prevent instantiation
    }

    public static final String TIMESTAMP = "timestamp";
    public static final String TIMESTAMP_2 = "timestamp2";
    public static final String VISIBILITY = "visibility";
    public static final String ID_STRING = "id.string";
    public static final String DIRECTED_EITHER = "directed.either";
    public static final String DIRECTED_TRUE = "directed.true";
    public static final String PROP_STRING = "prop.string";
    public static final String PROP_INTEGER = "prop.integer";
    public static final String PROP_LONG = "prop.long";
    public static final String PROP_INTEGER_2 = "prop.integer.2";
    public static final String PROP_COUNT = "prop.count";
    public static final String PROP_MAP = "prop.map";
    public static final String PROP_SET_STRING = "prop.set.string";
}
