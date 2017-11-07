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

/**
 * Utility class providing constants for the Gaffer REST API.
 */
public final class ServiceConstants {

    // REST Headers
    public static final String GAFFER_MEDIA_TYPE_HEADER = "X-Gaffer-Media-Type";
    public static final String GAFFER_MEDIA_TYPE;

    // REST status error messages
    public static final String OK = "OK";
    public static final String BAD_REQUEST = "Error while processing request body";
    public static final String FORBIDDEN = "The current user cannot perform the requested operation";
    public static final String INTERNAL_SERVER_ERROR = "Something went wrong in the server";

    public static final String CLASS_NOT_FOUND = "Class not found";
    public static final String FUNCTION_NOT_FOUND = "Function not found";
    public static final String PROPERTY_NOT_FOUND = "Property not found";

    public static final String OPERATION_NOT_FOUND = "Operation not found";
    public static final String OPERATION_NOT_IMPLEMENTED = "The requested operation is not supported by the target store";

    public static final String JOB_CREATED = "A new job was successfully submitted";
    public static final String JOB_NOT_FOUND = "Job was not found";
    public static final String JOB_SERVICE_UNAVAILABLE = "The job service is not available";

    // REST - Other
    public static final String DESCRIPTION_DEFAULT = SystemProperty.APP_DESCRIPTION_DEFAULT;
    public static final String GAFFER_DOCUMENTATION_URL = SystemProperty.APP_DOCUMENTATION_URL_DEFAULT;

    static {
        final String apiVersion = System.getProperty(SystemProperty.REST_API_VERSION, SystemProperty.CORE_VERSION);
        GAFFER_MEDIA_TYPE = "gaffer.v" + apiVersion.charAt(0) + "; format=json";
    }

    private ServiceConstants() {
        // Empty constructor to prevent instantiation.
    }
}
