/*
 * Copyright © 2019 Cask Data, Inc.
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

package io.cdap.plugin.decompress.action;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.api.plugin.PluginProperties;
import io.cdap.cdap.etl.api.FailureCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Config class for {@link DecompressAction}.
 */
public class DecompressActionConfig extends PluginConfig {
  public static final String SOURCE_FILE_PATH = "sourceFilePath";
  public static final String DEST_FILE_PATH = "destFilePath";
  public static final String FILE_REGEXP = "fileRegex";
  public static final String ARCHIVED_OR_COMPRESSED = "archivedOrCompressed";
  public static final String CONTINUE_ON_ERROR = "continueOnError";

  @Name(SOURCE_FILE_PATH)
  @Macro
  @Description("The source location where the file or files live. You can use glob syntax here such as *.gz.")
  private final String sourceFilePath;

  @Name(DEST_FILE_PATH)
  @Macro
  @Description("The destination location where the converted files should be.")
  private final String destFilePath;

  @Name(FILE_REGEXP)
  @Macro
  @Nullable
  @Description("A regular expression for filtering files such as .*\\.txt")
  private final String fileRegex;

  @Name(ARCHIVED_OR_COMPRESSED)
  @Macro
  @Description("Are you processing archived files (E.g. .tar or .zip), compressed files (E.g. .gz or .bz2), or " +
    "archived then compressed filed (E.g. .tar.gz)")
  private final String archivedOrCompressed;

  @Name(CONTINUE_ON_ERROR)
  @Macro
  @Nullable
  @Description("Set to true if this plugin should ignore errors.")
  private final Boolean continueOnError;


  public DecompressActionConfig(String sourceFilePath, String destFilePath, @Nullable String fileRegex,
                                String archivedOrCompressed, @Nullable Boolean continueOnError) {
    this.sourceFilePath = sourceFilePath;
    this.destFilePath = destFilePath;
    this.continueOnError = (continueOnError == null) ? false : continueOnError;
    this.archivedOrCompressed = archivedOrCompressed;
    this.fileRegex = (Strings.isNullOrEmpty(fileRegex)) ? ".*" : fileRegex;
  }

  private DecompressActionConfig(Builder builder) {
    sourceFilePath = builder.sourceFilePath;
    destFilePath = builder.destFilePath;
    fileRegex = builder.fileRegex;
    archivedOrCompressed = builder.archivedOrCompressed;
    continueOnError = builder.continueOnError;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(DecompressActionConfig copy) {
    Builder builder = new Builder();
    builder.sourceFilePath = copy.getSourceFilePath();
    builder.destFilePath = copy.getDestFilePath();
    builder.fileRegex = copy.getFileRegex();
    builder.archivedOrCompressed = copy.getArchivedOrCompressed();
    builder.continueOnError = copy.getContinueOnError();
    return builder;
  }

  public String getSourceFilePath() {
    return sourceFilePath;
  }

  public String getDestFilePath() {
    return destFilePath;
  }

  public String getArchivedOrCompressed() {
    return archivedOrCompressed;
  }

  @Nullable
  public Boolean getContinueOnError() {
    return continueOnError;
  }

  public String getFileRegex() {
    return (Strings.isNullOrEmpty(fileRegex)) ? ".*" : fileRegex;
  }

  /**
   * Validates the config parameters required for unloading the data.
   */
  public void validate(FailureCollector collector) {
    try {
      Pattern.compile(getFileRegex());
    } catch (Exception e) {
      collector.addFailure("The regular expression pattern provided is not a valid regular expression.", null)
        .withConfigProperty(FILE_REGEXP)
        .withStacktrace(e.getStackTrace());
    }

    if (!containsMacro(SOURCE_FILE_PATH) && Strings.isNullOrEmpty(sourceFilePath)) {
      collector.addFailure("Source file or folder is required.", null)
        .withConfigProperty(SOURCE_FILE_PATH);
    }
    if (!containsMacro(DEST_FILE_PATH) && Strings.isNullOrEmpty(destFilePath)) {
      collector.addFailure("Destination file or folder is required.", null)
        .withConfigProperty(DEST_FILE_PATH);
    }

    if (!containsMacro(ARCHIVED_OR_COMPRESSED) && (Strings.isNullOrEmpty(archivedOrCompressed) || (
      !archivedOrCompressed.toLowerCase().equals("archived") &&
        !archivedOrCompressed.toLowerCase().equals("compressed") &&
        !archivedOrCompressed.toLowerCase().equals("archived then compressed")))) {
      collector.addFailure("You must specify if you are processing archive files, compressed files, or both.", null)
        .withConfigProperty(ARCHIVED_OR_COMPRESSED);
    }

    if (!containsMacro(CONTINUE_ON_ERROR) && Objects.isNull(continueOnError)) {
      collector.addFailure("Continue on error must be specified.", null)
        .withConfigProperty(CONTINUE_ON_ERROR);
    }
  }


  public static final class Builder {
    private String sourceFilePath;
    private String destFilePath;
    private String fileRegex;
    private String archivedOrCompressed;
    private Boolean continueOnError;

    private Builder() {
    }

    public Builder setSourceFilePath(String sourceFilePath) {
      this.sourceFilePath = sourceFilePath;
      return this;
    }

    public Builder setDestFilePath(String destFilePath) {
      this.destFilePath = destFilePath;
      return this;
    }

    public Builder setFileRegex(String fileRegex) {
      this.fileRegex = fileRegex;
      return this;
    }

    public Builder setArchivedOrCompressed(String archivedOrCompressed) {
      this.archivedOrCompressed = archivedOrCompressed;
      return this;
    }

    public Builder setContinueOnError(Boolean continueOnError) {
      this.continueOnError = continueOnError;
      return this;
    }

    public DecompressActionConfig build() {
      return new DecompressActionConfig(this);
    }
  }
}
