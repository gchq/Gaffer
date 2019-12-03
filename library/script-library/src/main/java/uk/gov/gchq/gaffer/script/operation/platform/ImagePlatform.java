/*
 * Copyright 2019 Crown Copyright
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
package uk.gov.gchq.gaffer.script.operation.platform;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import uk.gov.gchq.gaffer.script.operation.container.Container;
import uk.gov.gchq.gaffer.script.operation.image.Image;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "class", defaultImpl = LocalDockerPlatform.class)
@JsonSubTypes({
        @JsonSubTypes.Type(value = LocalDockerPlatform.class)})
@JsonDeserialize

public interface ImagePlatform {

    /**
     * Builds an image
     *
     * @param scriptName             the name of the script being run
     * @param scriptParameters       the parameters of the script being run
     * @param pathToBuildFiles       the path to the directory containing the build files
     * @return the image
     */
    Image buildImage(String scriptName, Map<String, Object> scriptParameters, String pathToBuildFiles);

    /**
     * Creates a container
     *
     * @param image                  the image to create a container from
     * @param ip                     the ip the container is connected to
     * @return the container
     */
    Container createContainer(Image image, String ip);

    /**
     * Runs a container
     *
     * @param container              the container to run
     * @param inputData              the data to pass to the container
     * @return the result of the container
     */
    StringBuilder runContainer(Container container, Iterable inputData);
}
