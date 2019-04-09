/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package io.cdap.plugin;


import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveException;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.compress.archivers.ArchiveStreamFactory;
import org.apache.commons.compress.compressors.CompressorException;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Action to expand compressed or archived files before processing in a pipeline
 */

@Plugin(type = Action.PLUGIN_TYPE)
@Name("Decompress")
@Description("Action to expand compressed or archived files before processing")
public class DecompressAction extends Action {
  private static final Logger LOG = LoggerFactory.getLogger(DecompressAction.class);
  private static final int BUFFER_SIZE = 8024; // Matches default buffer size in IOUtils

  private DecompressActionConfig config;

  public DecompressAction(DecompressActionConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) throws IllegalArgumentException {
    config.validate();
  }

  @Override
  public void run(ActionContext context) throws Exception {
    config.validate();
    Path source = new Path(config.sourceFilePath);
    Path dest = new Path(config.destFilePath);

    FileSystem fileSystem = source.getFileSystem(new Configuration());
    fileSystem.mkdirs(dest.getParent());

    // Convert a single file
    if (fileSystem.exists(source) && fileSystem.getFileStatus(source).isFile()) {
      convertSingleFile(source, dest, fileSystem);
    } else {
      // Convert all the files in a directory
      PathFilter filter = new PathFilter() {
        private final Pattern pattern = Pattern.compile(config.getFileRegex());

        @Override
        public boolean accept(Path path) {
          return pattern.matcher(path.getName()).matches();
        }
      };
      FileStatus[] listFiles = fileSystem.globStatus(source, filter);
      if (listFiles == null || listFiles.length == 0 || (listFiles.length == 1 && listFiles[0].isDirectory())) {
        // try again without globbing action
        listFiles = fileSystem.listStatus(source, filter);
      }

      if (listFiles.length == 0) {
        LOG.warn("Not converting any files from source {} matching regular expression {}",
                 source.toString(), config.getFileRegex());
      }

      if (fileSystem.exists(dest) && fileSystem.isFile(dest)) {
        throw new IllegalArgumentException(
          String.format("Destination %s needs to be a directory since the source is a " +
                          "directory", config.destFilePath));
      }
      // create destination directory if necessary
      fileSystem.mkdirs(dest);

      for (FileStatus file : listFiles) {
        if (!file.isDirectory()) { // ignore directories
          source = file.getPath();
          convertSingleFile(source, dest, fileSystem);
        }
      }
    }
  }

  private void convertSingleFile(Path source, Path dest, FileSystem fileSystem)
    throws ArchiveException, CompressorException, IOException, IllegalArgumentException {
    switch (config.archivedOrCompressed.toLowerCase()) {
      case "compressed" :
        processCompressedFiles(source, dest, fileSystem);
        break;
      case "archived" :
        processArchiveFiles(source, dest, fileSystem);
        break;
      case "archived then compressed":
        Path uncompressedFile = processCompressedFiles(source, dest, fileSystem);
        processArchiveFiles(uncompressedFile, dest, fileSystem);
        fileSystem.delete(uncompressedFile, false);
        break;
      default:
        throw new IllegalArgumentException("archivedOrCompressed must be one of " +
                                             "'archived','compressed', or 'archive then compressed' " +
                                             "but was: " + config.archivedOrCompressed);
    }
  }

  private void processArchiveFiles(Path source, Path dest, FileSystem fileSystem) throws ArchiveException, IOException {
    Path destPathWithFolder = new Path(dest.toString() + "/" + source.getName().substring(0, source.getName().lastIndexOf(".")));
    fileSystem.mkdirs(destPathWithFolder);
    try (ArchiveInputStream input = new ArchiveStreamFactory()
      .createArchiveInputStream(new BufferedInputStream(fileSystem.open(source)))) {
      ArchiveEntry entry = input.getNextEntry();
      // iterates over entries in the archive file
      while (entry != null) {
        if (input.canReadEntryData(entry)) {
          Path actualDestPath = new Path(destPathWithFolder.toString() + "/" + entry.getName());
          if (!entry.isDirectory()) {
            // if the entry is a file, extracts it
            BufferedOutputStream out = new BufferedOutputStream(fileSystem.create(actualDestPath), BUFFER_SIZE);
            IOUtils.copy(input, out);
            out.close();
          } else {
            // if the entry is a directory, make the directory
            fileSystem.mkdirs(actualDestPath);
          }
        } else {
          LOG.warn(String.format("Archive entry is using a feature that is not supported yet. " +
                                   "Skipping this entry. Source: %s Dest: %s", source.toString(), dest.toString()));
        }
        entry = input.getNextEntry();
      }
    } catch (ArchiveException e) {
      if (!config.continueOnError) {
        throw new ArchiveException(String.format("Failed to expand archived files %s to %s", source.toString(), dest.toString()), e);
      }
      LOG.warn(String.format("Failed to expand archived files %s to %s", source.toString(), dest.toString()), e);
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw new IOException(String.format("Failed to expand archived files %s to %s", source.toString(), dest.toString()), e);
      }
      LOG.warn(String.format("Failed to expand archived files %s to %s", source.toString(), dest.toString()), e);
    }
  }

  private Path processCompressedFiles(Path source, Path dest, FileSystem fileSystem) throws CompressorException, IOException {
    Path actualDestPath = (fileSystem.isDirectory(dest))
      ? new Path(dest.toString() + "/" + source.getName().substring(0, source.getName().lastIndexOf(".")))
      : dest;
    try (CompressorInputStream input = new CompressorStreamFactory()
      .createCompressorInputStream(new BufferedInputStream(fileSystem.open(source)));
         BufferedOutputStream out = new BufferedOutputStream(fileSystem.create(actualDestPath), BUFFER_SIZE)) {
      IOUtils.copy(input, out);
    } catch (CompressorException e) {
      if (!config.continueOnError) {
        throw new CompressorException(String.format("Failed to expand compressed files %s to %s", source.toString(), dest.toString()), e);
      }
      LOG.warn(String.format("Failed to expand compressed files %s to %s", source.toString(), dest.toString()), e);
    } catch (IOException e) {
      if (!config.continueOnError) {
        throw new IOException(String.format("Failed to expand compressed files %s to %s", source.toString(), dest.toString()), e);
      }
      LOG.warn(String.format("Failed to expand compressed files %s to %s", source.toString(), dest.toString()), e);
    }
    return actualDestPath;
  }


  /**
   *  Config for the action to decompress gz files from a container on Azure Storage Blob service into another container
   */
  public static class DecompressActionConfig extends PluginConfig {
      @Macro
      @Description("The source location where the file or files live. You can use glob syntax here such as *.gz.")
      private String sourceFilePath;

      @Macro
      @Description("The destination location where the converted files should be.")
      private String destFilePath;

      @Macro
      @Nullable
      @Description("A regular expression for filtering files such as .*\\.txt")
      private String fileRegex;

      @Macro
      @Description("Are you processing archived files (E.g. .tar or .zip), compressed files (E.g. .gz or .bz2), or " +
        "archived then compressed filed (E.g. .tar.gz)")
      private String archivedOrCompressed;

      @Macro
      @Nullable
      @Description("Set to true if this plugin should ignore errors.")
      private Boolean continueOnError;


      public DecompressActionConfig(String sourceFilePath, String destFilePath, @Nullable String fileRegex,
                                    String archivedOrCompressed, @Nullable Boolean continueOnError) {
        this.sourceFilePath = sourceFilePath;
        this.destFilePath = destFilePath;
        this.continueOnError = (continueOnError == null) ? false : continueOnError;
        this.archivedOrCompressed = archivedOrCompressed;
        this.fileRegex = (Strings.isNullOrEmpty(fileRegex)) ? ".*" : fileRegex;
      }

      public String getFileRegex() {
        return (Strings.isNullOrEmpty(fileRegex)) ? ".*" : fileRegex;
      }

      /**
       * Validates the config parameters required for unloading the data.
       */
      private void validate() throws IllegalArgumentException {
        try {
          Pattern.compile(getFileRegex());
        } catch (Exception e) {
          throw new IllegalArgumentException("The regular expression pattern provided is not a valid " +
                                               "regular expression.", e);
        }
        if (Strings.isNullOrEmpty(sourceFilePath)) {
          throw new IllegalArgumentException("Source file or folder is required.");
        }
        if (Strings.isNullOrEmpty(destFilePath)) {
          throw new IllegalArgumentException("Destination file or folder is required.");
        }
        try {
          Path source = new Path(sourceFilePath);
          FileSystem fileSystem = source.getFileSystem(new Configuration());
        } catch (IOException e) {
          throw new IllegalArgumentException("Cannot determine the file system of the source file.", e);
        }
        if (Strings.isNullOrEmpty(archivedOrCompressed) || (
          !archivedOrCompressed.toLowerCase().equals("archived") &&
          !archivedOrCompressed.toLowerCase().equals("compressed") &&
          !archivedOrCompressed.toLowerCase().equals("archived then compressed"))) {
          throw new IllegalArgumentException("You must specify if you are processing archive files, compressed files, or both.");
        }
      }
  }
}
