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

package uk.gov.gchq.gaffer.operation;

import org.junit.Test;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.commonutil.VersionUtil;

import java.util.Set;

import static org.junit.Assert.fail;

public abstract class AbstractOperationsVersionTest {

    private final String packageString;

    private final Logger LOGGER = LoggerFactory.getLogger(AbstractOperationsVersionTest.class);

    public AbstractOperationsVersionTest(final String packageString) {
        this.packageString = packageString;
    }

    @Test
    public void shouldBeAnnotatedWithVersion() {
        // Given
        final Reflections reflections = new Reflections(packageString);
        final Set<Class<? extends Operation>> operationClasses = reflections.getSubTypesOf(Operation.class);

        // Then
        reflections.getSubTypesOf(Operation.class)
                .stream()
                .filter(cls -> !cls.isInterface())
                .filter(cls -> !cls.isAnnotationPresent(TestOperation.class))
                .forEach(cls -> {
                    final Since since = cls.getAnnotation(Since.class);
                    if (null == since) {
                        fail("Class: " + cls.getCanonicalName() + " does not have a version annotation.");
                    }

                    if (!VersionUtil.validateVersionString(since.version())) {
                        fail("v" + since.version() + " is not a valid version string.");
                    }

                    LOGGER.info("Class " + cls.getCanonicalName() + " registered since: v" + since.version());
                });
    }
}
