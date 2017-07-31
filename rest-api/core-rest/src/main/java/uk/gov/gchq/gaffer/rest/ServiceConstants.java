/*
 * Copyright 2017 Crown Copyright
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
package uk.gov.gchq.gaffer.rest;

public final class ServiceConstants {

    // REST Headers
    public static final String GAFFER_MEDIA_TYPE_HEADER = "X-Gaffer-Media-Type";
    public static final String GAFFER_MEDIA_TYPE;

    static {
        final String apiVersion = System.getProperty(SystemProperty.REST_API_VERSION, SystemProperty.CORE_VERSION);
        GAFFER_MEDIA_TYPE = "gaffer.v" + apiVersion.charAt(0) + "; format=json";
    }

    private ServiceConstants() {
        // Empty constructor to prevent instantiation.
    }
}
