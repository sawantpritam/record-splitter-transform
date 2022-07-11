@RecordSplitter
Feature: RecordSplitter transform - Verify GCS source data transfer using RecordSplitter transform

  @GCS_CSV_DELIMITED_TEST @BQ_SINK_TEST
  Scenario: To verify data is getting transferred from GCS to BigQuery sink plugin successfully with record splitter
    Given Open Datafusion Project to configure pipeline
    When Select plugin: "GCS" from the plugins list as: "Source"
    When Expand Plugin group in the LHS plugins list: "Transform"
    When Select plugin: "Record Splitter" from the plugins list as: "Transform"
    Then Connect plugins: "GCS" and "RecordSplitter" to establish connection
    When Expand Plugin group in the LHS plugins list: "Sink"
    When Select plugin: "BigQuery" from the plugins list as: "Sink"
    Then Connect plugins: "RecordSplitter" and "BigQuery" to establish connection
    Then Navigate to the properties page of plugin: "GCS"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "GCSReferenceName"
    Then Enter input plugin property: "path" with value: "gcsSourcePath"
    Then Select dropdown plugin property: "format" with option value: "csv"
    Then Click plugin property: "skipHeader"
    Then Click plugin property: "enableQuotedValues"
    Then Click on the Get Schema button
    Then Verify the Output Schema matches the Expected Schema: "recordSplitterValidFileOutputSchema"
    Then Validate "GCS" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "RecordSplitter"
    Then Enter input plugin property: "fieldToSplit" with value: "recordSplitterFieldToSplit"
    Then Enter input plugin property: "delimiter" with value: "recordSplitterDoubleSlashDelimiter"
    Then Enter input plugin property: "outputField" with value: "recordSplitterOutputField"
    Then Select Record Splitter plugin output schema action: "clear"
    Then Enter Record Splitter plugin outputSchema "recordSplitterValidOutputSchema"
    Then Validate "RecordSplitter" plugin properties
    Then Close the Plugin Properties page
    Then Navigate to the properties page of plugin: "BigQuery"
    Then Replace input plugin property: "projectId" with value: "projectId"
    Then Enter input plugin property: "datasetProjectId" with value: "projectId"
    Then Override Service account details if set in environment variables
    Then Enter input plugin property: "referenceName" with value: "BQReferenceName"
    Then Enter input plugin property: "dataset" with value: "dataset"
    Then Enter input plugin property: "table" with value: "bqTargetTable"
    Then Click plugin property: "truncateTable"
    Then Click plugin property: "updateTableSchema"
    Then Validate "BigQuery" plugin properties
    Then Close the Plugin Properties page
    Then Save the pipeline
    Then Preview and run the pipeline
    Then Wait till pipeline preview is in running state
    Then Open and capture pipeline preview logs
    Then Verify the preview run status of pipeline in the logs is "succeeded"
    Then Close the pipeline logs
    Then Close the preview
    Then Deploy the pipeline
    Then Run the Pipeline in Runtime
    Then Wait till pipeline is in running state
    Then Open and capture logs
    Then Verify the pipeline status is "Succeeded"
    Then Close the pipeline logs
    Then Validate OUT record count of record splitter is equal to IN record count of sink
