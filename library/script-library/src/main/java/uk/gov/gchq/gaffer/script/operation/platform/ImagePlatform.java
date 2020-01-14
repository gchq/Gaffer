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
import com.spotify.docker.client.exceptions.DockerException;
import org.codehaus.jackson.map.annotate.JsonDeserialize;

import uk.gov.gchq.gaffer.script.operation.container.Container;
import uk.gov.gchq.gaffer.script.operation.image.Image;

import java.io.IOException;
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
     * @throws Exception             exception if Image is not built
     */
    Image buildImage(String scriptName, Map<String, Object> scriptParameters, String pathToBuildFiles) throws Exception;

    /**
     * Creates a container
     *
     * @param image                  the image to create a container from
     * @return the container
     * @throws Exception             if it fails to create the container
     */
    Container createContainer(Image image) throws Exception;

    /**
     * Runs a container
     *
     * @param container              the container to run
     * @param inputData              the data to pass to the container
     * @throws Exception             exception if container does not run
     */
    void runContainer(Container container, Iterable inputData) throws Exception;

    void closeContainer(Container container);

    void startContainer(Container container) throws DockerException, InterruptedException, IOException;
}
