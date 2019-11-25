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
package uk.gov.gchq.gaffer.python.operation.handler;

import org.eclipse.jgit.api.Git;

import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.python.operation.DockerImageBuilder;
import uk.gov.gchq.gaffer.python.operation.GitScriptProvider;
import uk.gov.gchq.gaffer.python.operation.RunPythonScript;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;

import java.io.File;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;

public class RunPythonScriptHandler implements OperationHandler<RunPythonScript> {

    private final DockerImageBuilder dockerImageBuilder = new DockerImageBuilder();
    private final GitScriptProvider gitScriptProvider = new GitScriptProvider();
    private Git git = null;

    private String dockerfilePath = "";
    private String repoURI = "https://github.com/g609bmsma/test";
    private String repoName = "test";
    private String ip = "127.0.0.1";

    @Override
    public Object doOperation(final RunPythonScript operation, final Context context, final Store store) throws OperationException {

        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat("/src/main/resources/.PythonBin");
        final Path pathAbsolutePythonRepo = Paths.get(directoryPath, repoName);
        final File directory = new File(directoryPath);
        if (!directory.exists()) {
            directory.mkdir();
        }

        Object output = null;

        final String scriptName = operation.getScriptName();
        final Map<String, Object> scriptParameters = operation.getScriptParameters();

        // Pull or Clone the repo with the files
        gitScriptProvider.getScripts(git, pathAbsolutePythonRepo.toString(), repoURI);
        dockerImageBuilder.getFiles(directoryPath, dockerfilePath);

        return output;
    }

    private String getDockerfilePath() {
        return dockerfilePath;
    }

    private void setDockerfilePath(final String dockerfilePath) {
        this.dockerfilePath = dockerfilePath;
    }

    private String getRepoName() {
        return repoName;
    }

    private void setRepoName(final String repoName) {
        this.repoName = repoName;
    }

    private String getRepoURI() {
        return repoURI;
    }

    private void setRepoURI(final String repoURI) {
        this.repoURI = repoURI;
    }

    private String getIp() {
        return ip;
    }

    private void setIp(final String ip) {
        this.ip = ip;
    }
}
