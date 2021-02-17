/*
 * Copyright 2020 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.solutions.beam;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

/**
 * Creates a beam Pipeline that reads Avro records and writes the Avro records to GCS
 * Command to run this job:
 *
 ------------ JSON ------------
 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.PubsubTextToGcs \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --inputPath=projects/$YOUR-PROJ--poc-proj/subscriptions/sub_json \
 --outputPath=gs://$YOUR-PROJ-poc_data/ingested/json/run4/ \
 --workerMachineType=n1-highmem-4 \
 --usePublicIps=false \
 --numWorkers=80 \
 --maxNumWorkers=80 \
 --autoscalingAlgorithm=NONE \
 --diskSizeGb=30 \
 --enableStreamingEngine \
 --experiments=shuffle_mode=service \
 --region=us-central1 \
 --jobName=pubsub-json-to-gcs-run3-1" \
 --file BeamAvro/pom.xml

 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.PubsubTextToGcs \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --templateLocation=gs://$YOUR-PROJ-poc_dataflow/templates/PubsubJsonToGcs_v1.0 \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --workerMachineType=n1-highmem-4 \
 --usePublicIps=false \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --autoscalingAlgorithm=NONE \
 --diskSizeGb=30 \
 --enableStreamingEngine \
 --experiments=shuffle_mode=service \
 --region=us-central1" \
 --file BeamAvro/pom.xml



 ------------ CSV ------------
 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.PubsubTextToGcs \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --inputPath=projects/$YOUR-PROJ--poc-proj/subscriptions/sub_csv \
 --outputPath=gs://$YOUR-PROJ--sample-data-files/output/run1/ \
 --workerMachineType=n1-standard-4 \
 --usePublicIps=false \
 --numWorkers=80 \
 --maxNumWorkers=80 \
 --autoscalingAlgorithm=NONE \
 --diskSizeGb=30 \
 --enableStreamingEngine \
 --experiments=shuffle_mode=service \
 --region=us-central1 \
 --jobName=pubsub-csv-to-gcs-run5" \
 --file BeamAvro/pom.xml


 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.PubsubTextToGcs \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --templateLocation=gs://$YOUR-PROJ-poc_dataflow/templates/PubsubCsvToGcs_v1.0 \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --workerMachineType=n1-standard-4 \
 --usePublicIps=false \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --autoscalingAlgorithm=NONE \
 --diskSizeGb=30 \
 --enableStreamingEngine \
 --experiments=shuffle_mode=service \
 --region=us-central1" \
 --file BeamAvro/pom.xml
 */

public class PubsubTextToGcs {
    public interface TextOptions extends PipelineOptions, DataflowPipelineOptions {
        @Description("Input path")
        ValueProvider<String> getInputPath();

        void setInputPath(ValueProvider<String> path);

        @Description("Output path")
        ValueProvider<String> getOutputPath();

        void setOutputPath(ValueProvider<String> path);

        @Description("suffix")
        ValueProvider<String> getSuffix();

        void setSuffix(ValueProvider<String> suffix);
    }


    public static void main(String[] args) {
        TextOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(TextOptions.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);
        // Read String objects from PubSub
        PCollection<String> ods = pipeline.apply(
                "Read Text from PubSub", PubsubIO.readStrings().fromSubscription(options.getInputPath()));

        // Write to GCS
        // [START gcs_write]
        ods.apply("Window for 10 seconds", Window.into(FixedWindows.of(Duration.standardSeconds(10))))
               /// .apply("Custom Write Fn", ParDo.of(new WriteUnshardedFiles()))
                .apply(
                        "Write Text file",
                        TextIO.write().to(options.getOutputPath())
                                .withSuffix(".csv")
                                .withWindowedWrites().withNumShards(320));
        // [END gcs_write]
        pipeline.run();
    }
}
