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

package uk.gov.gchq.gaffer.python.operation;

import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.errors.RepositoryNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class PullOrCloneRepo {
    private static final Logger LOGGER = LoggerFactory.getLogger(PullOrCloneRepo.class);

    public synchronized void pullOrClone(final Git git, final String pathAbsolutePythonRepo,
                             final RunPythonScript operation) {
        String repoURI = operation.getRepoURI();
        Git newGit = git;
        if (git == null) {
            try {
                newGit = Git.open(new File(pathAbsolutePythonRepo));
            } catch (final RepositoryNotFoundException e) {
                try {
                    newGit = Git.cloneRepository().setDirectory(new File(pathAbsolutePythonRepo)).setURI(repoURI).call();
                } catch (final GitAPIException e1) {
                    e1.printStackTrace();
                    newGit = null;
                }
            } catch (final IOException e) {
                e.printStackTrace();
                newGit = null;
            }
        }
        LOGGER.info("Fetching the repo.");
        File dir = new File(pathAbsolutePythonRepo);
        try {
            if (newGit != null) {
                LOGGER.info("Repo already cloned, pulling files...");
                newGit.pull().call();
                LOGGER.info("Pulled the latest files.");
            } else {
                LOGGER.info("Repo has not been cloned, cloning the repo...");
                Git.cloneRepository().setDirectory(dir).setURI(repoURI).call();
                LOGGER.info("Cloned the repo");
            }
        } catch (final GitAPIException e) {
            e.printStackTrace();
        }
    }
}
