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

package gaffer.rest.application;

import gaffer.rest.SystemProperty;
import gaffer.rest.serialisation.RestJsonProvider;
import gaffer.rest.service.SimpleExamplesService;
import gaffer.rest.service.SimpleGraphConfigurationService;
import gaffer.rest.service.SimpleOperationService;
import gaffer.rest.service.StatusService;
import io.swagger.jaxrs.config.BeanConfig;
import io.swagger.jaxrs.listing.ApiListingResource;
import io.swagger.jaxrs.listing.SwaggerSerializers;
import javax.ws.rs.core.Application;
import java.util.HashSet;
import java.util.Set;

/**
 * An <code>ApplicationConfig</code> sets up the application resources and singletons.
 */
public class ApplicationConfig extends Application {
    protected final Set<Object> singletons = new HashSet<>();
    protected final Set<Class<?>> resources = new HashSet<>();

    public ApplicationConfig() {
        addSystemResources();
        addServices();
        setupBeanConfig();
    }

    protected void setupBeanConfig() {
        BeanConfig beanConfig = new BeanConfig();
        beanConfig.setSchemes(new String[]{"http"});
        beanConfig.setHost(System.getProperty(SystemProperty.HOST_AND_PORT, SystemProperty.HOST_AND_PORT_DEFAULT));
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
        resources.add(SimpleOperationService.class);
        resources.add(SimpleGraphConfigurationService.class);
        resources.add(SimpleExamplesService.class);
    }

    protected void addSystemResources() {
        resources.add(ApiListingResource.class);
        resources.add(SwaggerSerializers.class);
        resources.add(RestJsonProvider.class);
    }

    @Override
    public Set<Class<?>> getClasses() {
        return resources;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}
