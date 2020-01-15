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

package uk.gov.gchq.gaffer.script.operation;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.eclipse.jgit.api.Git;
import org.eclipse.jgit.api.errors.GitAPIException;
import org.eclipse.jgit.http.server.GitServlet;
import org.eclipse.jgit.lib.Repository;
import org.eclipse.jgit.storage.file.FileRepositoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;


public class GitServerUtils extends Thread {
    private static final Logger LOGGER = LoggerFactory.getLogger(GitServerUtils.class);
    private static Server server = new Server(8080);
    private static boolean setupAndStart;
    private static Path pathAbsoluteScriptRepo;

    public GitServerUtils(Path repoPath) {
        if (null == repoPath || repoPath.toString().isEmpty()) {
            setupAndStart = true;
        } else {
            setupAndStart = false;
            pathAbsoluteScriptRepo = repoPath;
        }
    }

    public void run() {
        setupAndStartServer();
    }

    public void setupAndStartServer() {
        if (setupAndStart) {
            setupAndStart();
        } else {
            startExistingServer(pathAbsoluteScriptRepo);
        }
    }

    private void setupAndStart() {
        try {
            Repository repository = createNewRepository();
            GitServlet gs = getGitServlet(repository);

            populateRepository(repository);
            startServer(gs);

        } catch (Exception e) {
            LOGGER.error("setupAndStart(): " + e.getMessage());
        }
    }

    public void startExistingServer(Path pathAbsoluteScriptRepo) {

        try {
            Repository repository = getRepository(pathAbsoluteScriptRepo);
            GitServlet gs = getGitServlet(repository);
            startServer(gs);

        } catch (Exception e) {
            LOGGER.error("startExistingServer(): " + e.getMessage());
        }
    }

    private GitServlet getGitServlet(Repository repository) {
        // Create the JGit Servlet which handles the Git protocol
        GitServlet gs = new GitServlet();
        gs.setRepositoryResolver((req, name) -> {
            repository.incrementOpen();
            return repository;
        });
        return gs;
    }

    private void startServer(GitServlet gs) throws Exception {
        try {
            // start up the Servlet and start serving requests
            configureAndStartHttpServer(gs);

            server.join();
        } catch (Exception e) {
            LOGGER.error("startServer(): " + e.getMessage());
        }
    }

    public void stopServer() throws Exception {
        server.stop();
    }

    private void configureAndStartHttpServer(GitServlet gs) throws Exception {

        ServletHandler handler = new ServletHandler();
        server.setHandler(handler);

        ServletHolder holder = new ServletHolder(gs);

        handler.addServletWithMapping(holder, "/*");

        server.start();
    }

    private void populateRepository(Repository repository) throws IOException, GitAPIException, Exception {
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        final Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_NAME);

        try {
            Git git = Git.init().setDirectory(new File(pathAbsoluteScriptRepo.toString())).setBare(false).call();
            git.open(new File(pathAbsoluteScriptRepo.toString()));
            File dir = new File(pathAbsoluteScriptRepo.toString());
            File[] directoryListing = dir.listFiles(new FilenameFilter() {
                public boolean accept(File dir, String name) {
                    return name.toLowerCase().endsWith(".txt") || name.toLowerCase().endsWith(".py");
                }
            });

            if (directoryListing != null) {
                for (File child : directoryListing) {
                    try {
                        git.add().addFilepattern(child.getName()).call();
                        git.commit().setMessage("Test-Checkin").call();
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            } else {
                throw new IOException("There are no files to add.");
            }
        } catch (Exception e) {
            throw new Exception("populateRepository failed: " + e.getMessage());
        }
    }

    private Repository createNewRepository() throws IOException {
        Repository repository = null;
        final String currentWorkingDirectory = FileSystems.getDefault().getPath("").toAbsolutePath().toString();
        final String directoryPath = currentWorkingDirectory.concat(ScriptTestConstants.CURRENT_WORKING_DIRECTORY);
        final Path pathAbsoluteScriptRepo = DockerFileUtils.getPathAbsoluteScriptRepo(directoryPath, ScriptTestConstants.REPO_DIR);

        // prepare a new folder
        try {
            File dirPath = new File(String.valueOf(pathAbsoluteScriptRepo));
            File oldRepo = new File(pathAbsoluteScriptRepo.toString() + "/" + ScriptTestConstants.REPO_NAME);

            CheckDirectoryExists(dirPath, oldRepo);

            // create the directory
            repository = new FileRepositoryBuilder()
                                    .setGitDir(oldRepo)
                                    .readEnvironment()
                                    .findGitDir()
                                    .build();

            repository.create();
        } catch (IOException e) {
            throw new IOException("createNewRepository: {}" + e.getMessage());
        } finally {
            return repository;
        }
    }

    private Repository getRepository(Path pathAbsoluteScriptRepo) throws IOException {
        Repository repository = null;
        pathAbsoluteScriptRepo = Paths.get(pathAbsoluteScriptRepo.toString() + "/.git");

        try {
            FileRepositoryBuilder repositoryBuilder = new FileRepositoryBuilder();
            repositoryBuilder.setMustExist(true);
            repositoryBuilder.setGitDir(pathAbsoluteScriptRepo.toFile());
            repository = repositoryBuilder.build();

        } catch (IOException e) {
            throw new IOException("getRepository: {}" + e.getMessage());
        } finally {
            return repository;
        }
    }
    private void CheckDirectoryExists(File dirPath, File oldRepo) throws IOException {
        if (!dirPath.exists()) {
            dirPath.mkdir();
        } else if (oldRepo.exists()) {
            DeleteFilesAndDirectories(oldRepo);
        }
    }

    private void DeleteFilesAndDirectories(File oldRepo) throws IOException {
        Path directory = Paths.get(String.valueOf(oldRepo));
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
