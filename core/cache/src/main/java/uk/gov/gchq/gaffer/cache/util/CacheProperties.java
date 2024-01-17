/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.cache.util;

/**
 * System properties used by the Gaffer cache service implementations.
 */
public final class CacheProperties {

    private CacheProperties() {
        // private constructor to prevent instantiation
    }

    /**
     * Name of a system property previously used for defining the default cache service class.
     * @deprecated Use {@link #CACHE_SERVICE_DEFAULT_CLASS} instead.
     */
    @Deprecated
    public static final String CACHE_SERVICE_CLASS = "gaffer.cache.service.class";

    /**
     * Name of the system property to use for defining the default cache service class.
     */
    public static final String CACHE_SERVICE_DEFAULT_CLASS = "gaffer.cache.service.default.class";

    /**
     * Name of the system property to use for defining a cache service class dedicated to the Job Tracker.
     */
    public static final String CACHE_SERVICE_JOB_TRACKER_CLASS = "gaffer.cache.service.jobtracker.class";

    /**
     * Name of the system property to use for defining a cache service class dedicated to Named Views.
     */
    public static final String CACHE_SERVICE_NAMED_VIEW_CLASS = "gaffer.cache.service.namedview.class";

    /**
     * Name of the system property to use for defining a cache service class dedicated to Named Operations.
     */
    public static final String CACHE_SERVICE_NAMED_OPERATION_CLASS = "gaffer.cache.service.namedoperation.class";

    /**
     * Name of the system property to use in order to locate the cache config file.
     */
    public static final String CACHE_CONFIG_FILE = "gaffer.cache.config.file";

}
