/*
 * Copyright 2017. Crown Copyright
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

package uk.gov.gchq.gaffer.parquetstore.utils;

import uk.gov.gchq.gaffer.data.element.IdentifierType;

/**
 *
 */
public final class Constants {
    public static final String GROUP = "GROUP";
    public static final String VERTEX = IdentifierType.VERTEX.toString();
    public static final String SOURCE = IdentifierType.SOURCE.toString();
    public static final String DESTINATION = IdentifierType.DESTINATION.toString();
    public static final String DIRECTED = IdentifierType.DIRECTED.toString();

    private Constants() {
    }
}
