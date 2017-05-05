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
package uk.gov.gchq.gaffer.doc.user.walkthrough;

import uk.gov.gchq.gaffer.doc.user.generator.RoadUseCsvGenerator;
import uk.gov.gchq.gaffer.doc.util.JavaSourceUtil;
import uk.gov.gchq.gaffer.doc.walkthrough.WalkthroughStrSubstitutor;
import uk.gov.gchq.gaffer.traffic.transform.DescriptionTransform;
import java.util.HashMap;
import java.util.Map;

public abstract class UserWalkthroughStrSubstitutor extends WalkthroughStrSubstitutor {
    public static String substitute(final String text, final UserWalkthrough example) {
        return substitute(text, createParameterMap(example));
    }

    public static Map<String, String> createParameterMap(final UserWalkthrough example) {
        final Map<String, String> params = new HashMap<>();
        params.put("ROAD_TRAFFIC_EXAMPLE_LINK", getGitHubFileLink("Road Traffic Example", "example/road-traffic/README.md"));
        params.put("CSV_GENERATOR_JAVA",
                JavaSourceUtil.getJava(RoadUseCsvGenerator.class.getName(), "doc"));
        params.put("DESCRIPTION_TRANSFORM_LINK", getGitHubCodeLink(DescriptionTransform.class, "example/road-traffic-model"));
        params.put("ROAD_TRAFFIC_SAMPLE_DATA_LINK", getGitHubFileLink("Road Traffic Sample ", "example/road-traffic/road-traffic-demo/src/main/java/resources/roadTrafficSampleData.csv"));
        return params;
    }
}
