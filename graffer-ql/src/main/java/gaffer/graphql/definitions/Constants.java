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
package gaffer.graphql.definitions;

/**
 * Encapsulate the constants used in GraphQL
 */
public final class Constants {

    /**
     * Names for the fixed Gaffer properties for representation in GraphQL
     */
    public static final String ENTITY = "Entity";
    public static final String EDGE = "Edge";
    public static final String VERTEX = "vertex";
    public static final String VERTEX_VALUE = "vertex_value";
    public static final String VALUE = "value";
    public static final String SOURCE = "source";
    public static final String DESTINATION = "destination";
    public static final String SOURCE_VALUE = "source_value";
    public static final String DESTINATION_VALUE = "destination_value";

    private Constants() {

    }
}
