/*
 * Copyright © 2016-2019 Cask Data, Inc.
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

package io.cdap.plugin.record.splitter;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;

import java.io.IOException;

/**
 * Record Splitter plugin configuration.
 */
public class RecordSplitterConfig extends PluginConfig {
  public static final String FIELD_TO_SPLIT = "fieldToSplit";
  public static final String DELIMITER = "delimiter";
  public static final String OUTPUT_FIELD = "outputField";
  public static final String SCHEMA = "schema";

  @Name(FIELD_TO_SPLIT)
  @Description("Specifies the field to split.")
  @Macro
  private String fieldToSplit;

  @Name(DELIMITER)
  @Description("Specifies the delimiter used to split each record. If using escape characters, " +
    "be sure to double escape. So \n = \\n")
  @Macro
  private String delimiter;

  @Name(OUTPUT_FIELD)
  @Description("Specifies the name of the output field.")
  @Macro
  private String outputField;

  @Name(SCHEMA)
  @Description("Specifies the schema that has to be output.")
  private String schema;

  public RecordSplitterConfig(String fieldToSplit, String delimiter, String outputField, String schema) {
    this.fieldToSplit = fieldToSplit;
    this.delimiter = delimiter;
    this.outputField = outputField;
    this.schema = schema;
  }

  private RecordSplitterConfig(Builder builder) {
    fieldToSplit = builder.fieldToSplit;
    delimiter = builder.delimiter;
    outputField = builder.outputField;
    schema = builder.schema;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(RecordSplitterConfig copy) {
    return builder()
      .setFieldToSplit(copy.fieldToSplit)
      .setDelimiter(copy.delimiter)
      .setOutputField(copy.outputField)
      .setSchema(copy.schema);
  }

  public String getFieldToSplit() {
    return fieldToSplit;
  }

  public String getDelimiter() {
    return delimiter;
  }

  public String getOutputField() {
    return outputField;
  }

  public Schema getSchema() {
    try {
      return Schema.parseJson(schema);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to parse output schema.", e);
    }
  }

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    try {
      Schema outSchema = Schema.parseJson(schema);
      if (!containsMacro(OUTPUT_FIELD) && outSchema.getField(outputField) == null) {
        failureCollector.addFailure("Output schema must contain the specified output field.", null)
          .withConfigProperty(OUTPUT_FIELD);
      }
    } catch (IOException e) {
      failureCollector.addFailure("Unable to parse output schema.", null)
        .withStacktrace(e.getStackTrace())
        .withConfigProperty(SCHEMA);
      return;
    }
    if (!containsMacro(FIELD_TO_SPLIT)) {
      Schema.Field field = inputSchema.getField(fieldToSplit);
      if (field == null) {
        failureCollector.addFailure(
          String.format("Source field: '%s' must be present in input schema.", fieldToSplit), null)
          .withConfigProperty(FIELD_TO_SPLIT);
        return;
      }
      Schema inputFieldSchema = inputSchema.getField(fieldToSplit).getSchema();

      Schema.Type inputFieldType = inputFieldSchema.isNullable()
        ? inputFieldSchema.getNonNullable().getType()
        : inputFieldSchema.getType();
      if (inputFieldType != Schema.Type.STRING) {
        failureCollector.addFailure(
          String.format("Source field: '%s' must be of type '%s', but is %s", fieldToSplit,
                        Schema.of(Schema.Type.STRING).getDisplayName(), inputFieldType.name()),
          null)
          .withInputSchemaField(fieldToSplit)
          .withConfigProperty(FIELD_TO_SPLIT);
      }
    }
  }

  /**
   * Builder for RecordSplitterConfig
   */
  public static final class Builder {
    private String fieldToSplit;
    private String delimiter;
    private String outputField;
    private String schema;

    private Builder() {
    }

    public Builder setFieldToSplit(String fieldToSplit) {
      this.fieldToSplit = fieldToSplit;
      return this;
    }

    public Builder setDelimiter(String delimiter) {
      this.delimiter = delimiter;
      return this;
    }

    public Builder setOutputField(String outputField) {
      this.outputField = outputField;
      return this;
    }

    public Builder setSchema(String schema) {
      this.schema = schema;
      return this;
    }

    public RecordSplitterConfig build() {
      return new RecordSplitterConfig(this);
    }
  }
}
