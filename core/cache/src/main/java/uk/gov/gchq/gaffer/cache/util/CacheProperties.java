/*
 * Copyright 2016-2024 Crown Copyright
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
     * Names of the system properties used to set the suffix for all caches or per cache.
     * CASE INSENSITIVE
     * e.g. gaffer.cache.service.default.suffix="v2"
     */
    public static final String CACHE_SERVICE_DEFAULT_SUFFIX = "gaffer.cache.service.default.suffix";
    public static final String CACHE_SERVICE_NAMED_OPERATION_SUFFIX = "gaffer.cache.service.named.operation.suffix";
    public static final String CACHE_SERVICE_JOB_TRACKER_SUFFIX = "gaffer.cache.service.job.tracker.suffix";
    public static final String CACHE_SERVICE_NAMED_VIEW_SUFFIX = "gaffer.cache.service.named.view.suffix";

    /**
     * Name of the system property to use in order to locate the cache config file.
     */
    public static final String CACHE_CONFIG_FILE = "gaffer.cache.config.file";

}
