/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.application;

import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.glassfish.jersey.server.ResourceConfig;
import uk.gov.gchq.gaffer.rest.FactoriesBinder;
import uk.gov.gchq.gaffer.rest.mapper.GafferCheckedExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.GafferRuntimeExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.GenericExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.ProcessingExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.WebApplicationExceptionMapper;
import uk.gov.gchq.gaffer.rest.serialisation.RestJsonProvider;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>ApplicationConfig</code> sets up the application resources.
 */
public abstract class ApplicationConfig extends ResourceConfig {
    protected final Set<Class<?>> resources = new HashSet<>();

    public ApplicationConfig() {
        addSystemResources();
        addExceptionMappers();
        addServices();
        setupBeanConfig();
        registerClasses(resources);
        register(new FactoriesBinder());
        additionalConfig();
    }

    protected void addSystemResources() {
        resources.add(ApiListingResource.class);
        resources.add(SwaggerSerializers.class);
        resources.add(RestJsonProvider.class);
    }

    protected void addExceptionMappers() {
        resources.add(GafferCheckedExceptionMapper.class);
        resources.add(GafferRuntimeExceptionMapper.class);
        resources.add(ProcessingExceptionMapper.class);
        resources.add(WebApplicationExceptionMapper.class);
        resources.add(GenericExceptionMapper.class);
    }

    protected abstract void addServices();

    protected abstract void setupBeanConfig();

    /**
     * Empty method which can be overridden in order to carry out additional
     * configuration. These changes will be applied to all versions of the API
     * which are deployed.
     */
    public void additionalConfig() {
        // Empty by default
    }

}
