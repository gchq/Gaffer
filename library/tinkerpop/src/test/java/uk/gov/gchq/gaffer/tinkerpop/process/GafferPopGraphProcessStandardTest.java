/*
 * Copyright 2023 Crown Copyright
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

package uk.gov.gchq.gaffer.tinkerpop.process;

import org.apache.tinkerpop.gremlin.GraphProviderClass;
import org.apache.tinkerpop.gremlin.process.ProcessLimitedStandardSuite;
import org.junit.runner.RunWith;

import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraph;
import uk.gov.gchq.gaffer.tinkerpop.GafferPopGraphProvider;

/**
 * Executes the Standard Gremlin Process Test Suite using GafferPopGraph.
 * See {@link GafferPopGraphProvider} for default graph configuration.
 */
@RunWith(ProcessLimitedStandardSuite.class)
@GraphProviderClass(provider = GafferPopGraphProvider.class, graph = GafferPopGraph.class)
public class GafferPopGraphProcessStandardTest {
    // No body as running standard test suite
}
