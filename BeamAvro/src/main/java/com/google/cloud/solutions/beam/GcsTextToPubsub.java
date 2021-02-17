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
import org.apache.beam.sdk.transforms.Watch;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;


/**
 * Creates a beam Pipeline that reads json/csv files from GCS and streams records into Pubsub.
 * Command to run this job
 *---------- CSV --------------
 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.GcsTextToPubsub \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --inputPath=gs://$YOUR-PROJ--sample-data-files/input_wik/*.csv \
 --outputPath=projects/$YOUR-PROJ--poc-proj/topics/topic_csv \
 --workerMachineType=n1-highmem-8 \
 --numWorkers=100 \
 --maxNumWorkers=100 \
 --region=us-central1 \
 --jobName=gcs-csv-to-pubsub-run-1" \
 --file BeamAvro/pom.xml

-------------- JSON ----------------
 mvn clean generate-sources compile exec:java \
 -Dexec.mainClass=com.google.cloud.solutions.beam.GcsTextToPubsub \
 -Dexec.cleanupDaemonThreads=false \
 -Dexec.args=" \
 --project=$YOUR-PROJ--poc-proj \
 --runner=DataflowRunner \
 --stagingLocation=gs://$YOUR-PROJ-poc_dataflow/stage/ \
 --tempLocation=gs://$YOUR-PROJ-poc_dataflow/temp/ \
 --inputPath=gs://$YOUR-PROJ-poc_data/raw2/json/*.json \
 --outputPath=projects/$YOUR-PROJ--poc-proj/topics/topic_json \
 --workerMachineType=n1-highmem-8 \
 --numWorkers=40 \
 --maxNumWorkers=40 \
 --region=us-central1 \
 --jobName=gcs-json-to-pubsub-run1-2" \
 --file BeamAvro/pom.xml
 */
public class GcsTextToPubsub {
    private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

    public interface GcsAvroOptions extends PipelineOptions, DataflowPipelineOptions {
        @Description("Input path")
        @Validation.Required
        ValueProvider<String> getInputPath();
        void setInputPath(ValueProvider<String> path);

        @Description("Output path")
        @Validation.Required
        ValueProvider<String> getOutputPath();
        void setOutputPath(ValueProvider<String> path);
    }


    public static void main(String[] args) {
        GcsAvroOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(GcsAvroOptions.class);

        options.setStreaming(true);

        Pipeline pipeline = Pipeline.create(options);
        // Read text objects from Gcs
        PCollection<String> ods =
                pipeline.apply(
                        "Read Text from GCS",
                        TextIO.read()
                                .from(options.getInputPath())
                                .watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.never()));

        ods.apply("Write to Pubsub", PubsubIO.writeStrings().to(options.getOutputPath()));

        pipeline.run();
    }
}
