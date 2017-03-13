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

import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import org.glassfish.jersey.server.ResourceConfig;
import uk.gov.gchq.gaffer.rest.FactoriesBinder;
import uk.gov.gchq.gaffer.rest.SystemProperty;
import uk.gov.gchq.gaffer.rest.mapper.GafferCheckedExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.GafferRuntimeExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.GenericExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.ProcessingExceptionMapper;
import uk.gov.gchq.gaffer.rest.mapper.WebApplicationExceptionMapper;
import uk.gov.gchq.gaffer.rest.serialisation.RestJsonProvider;
import uk.gov.gchq.gaffer.rest.service.ExamplesService;
import uk.gov.gchq.gaffer.rest.service.GraphConfigurationService;
import uk.gov.gchq.gaffer.rest.service.JobService;
import uk.gov.gchq.gaffer.rest.service.OperationService;
import uk.gov.gchq.gaffer.rest.service.StatusService;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>ApplicationConfig</code> sets up the application resources.
 */
public class ApplicationConfig extends ResourceConfig {
    protected final Set<Class<?>> resources = new HashSet<>();

    public ApplicationConfig() {
        addSystemResources();
        addServices();
        addExceptionMappers();
        setupBeanConfig();
        registerClasses(resources);
        register(new FactoriesBinder());
    }

    protected void setupBeanConfig() {
        BeanConfig beanConfig = new BeanConfig();
        String baseUrl = System.getProperty(SystemProperty.BASE_URL, SystemProperty.BASE_URL_DEFAULT);
        if (!baseUrl.startsWith("/")) {
            baseUrl = "/" + baseUrl;
        }
        beanConfig.setBasePath(baseUrl);
        beanConfig.setVersion(System.getProperty(SystemProperty.VERSION, SystemProperty.CORE_VERSION));
        beanConfig.setResourcePackage(System.getProperty(SystemProperty.SERVICES_PACKAGE_PREFIX, SystemProperty.SERVICES_PACKAGE_PREFIX_DEFAULT));
        beanConfig.setScan(true);
    }

    protected void addServices() {
        resources.add(StatusService.class);
        resources.add(JobService.class);
        resources.add(OperationService.class);
        resources.add(GraphConfigurationService.class);
        resources.add(ExamplesService.class);
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

}
