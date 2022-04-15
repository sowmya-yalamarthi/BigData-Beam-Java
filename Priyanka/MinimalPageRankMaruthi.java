/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.nwmissouri.group03.Maruthi;

// beam-playground:
//   name: MinimalWordCount
//   description: An example that counts words in Shakespeare's works.
//   multifile: false
//   pipeline_options:
//   categories:
//     - Combiners
//     - Filtering
//     - IO
//     - Core Transforms

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/**
 * An example that counts words in Shakespeare.
 *
 * <p>This class, {@link MinimalWordCount}, is the first in a series of four successively more
 * detailed 'word count' examples. Here, for simplicity, we don't show any error-checking or
 * argument processing, and focus on construction of the pipeline, which chains together the
 * application of core transforms.
 *
 * <p>Next, see the {@link WordCount} pipeline, then the {@link DebuggingWordCount}, and finally the
 * {@link WindowedWordCount} pipeline, for more detailed examples that introduce additional
 * concepts.
 *
 * <p>Concepts:
 *
 * <pre>
 *   1. Reading data from text files
 *   2. Specifying 'inline' transforms
 *   3. Counting items in a PCollection
 *   4. Writing data to text files
 * </pre>
 *
 * <p>No arguments are required to run this pipeline. It will be executed with the DirectRunner. You
 * can see the results in the output files in your current working directory, with names like
 * "wordcounts-00001-of-00005. When running on a distributed service, you would use an appropriate
 * file service.
 */
public class MinimalPageRankMaruthi {

  public static void main(String[] args) {

    // Create a PipelineOptions object. This object lets us set various execution
    // options for our pipeline, such as the runner you wish to use. This example
    // will run with the DirectRunner by default, based on the class path configured
    // in its dependencies.
    PipelineOptions options = PipelineOptionsFactory.create();

    // In order to run your pipeline, you need to make following runner specific changes:
    //
    // CHANGE 1/3: Select a Beam runner, such as BlockingDataflowRunner
    // or FlinkRunner.
    // CHANGE 2/3: Specify runner-required options.
    // For BlockingDataflowRunner, set project and temp location as follows:
    //   DataflowPipelineOptions dataflowOptions = options.as(DataflowPipelineOptions.class);
    //   dataflowOptions.setRunner(BlockingDataflowRunner.class);
    //   dataflowOptions.setProject("SET_YOUR_PROJECT_ID_HERE");
    //   dataflowOptions.setTempLocation("gs://SET_YOUR_BUCKET_NAME_HERE/AND_TEMP_DIRECTORY");
    // For FlinkRunner, set the runner as follows. See {@code FlinkPipelineOptions}
    // for more details.
    //   options.as(FlinkPipelineOptions.class)
    //      .setRunner(FlinkRunner.class);

    // Create the Pipeline object with the options we defined above
    Pipeline p = Pipeline.create(options);

    // Concept #1: Apply a root transform to the pipeline; in this case, TextIO.Read to read a set
    // of input text files. TextIO.Read returns a PCollection where each element is one line from
    // the input text (a set of Shakespeare's texts).

    // This example reads from a public dataset containing the text of King Lear.
    //
    // DC: We don't need king lear....
    // We want to read from a folder - assign to a variable since it may change.
    // We want to read from a file - just one - we need the file name - assign to a variable. 

    String dataFolder = "web04";
    String dataFile = "go.md";
    String dataPath = dataFolder + "/" + dataFile;
    PCollection<KV<String, String>> p1 = Maruthi_Mapper(p, dataFolder, "go.md");
    PCollection<KV<String, String>> p2 = Maruthi_Mapper(p, dataFolder, "python.md");
    PCollection<KV<String, String>> p3 = Maruthi_Mapper(p, dataFolder, "java.md");
    PCollection<KV<String, String>> p4 = Maruthi_Mapper(p, dataFolder, "README.md");


        PCollectionList<KV<String, String>> pList1= PCollectionList.of(p1).and(p2).and(p3).and(p4);

        //add flatten

      PCollection<KV<String, String>> pList2 = pList1.apply(Flatten.<KV<String, String>>pCollections());
    PCollection<String> pList3 = pList2.apply(
        MapElements.into(
            TypeDescriptors.strings())
            .via((plist3out) -> plist3out.toString()));

            

    pList3.apply(TextIO.write().to("Maruthi_output"));

    
  
  }

  private static PCollection<KV<String, String>> Maruthi_Mapper(Pipeline p, String dataFolder, String dataFile) {
    String place = dataFolder + "/" + dataFile;
    PCollection<String> Lines = p.apply(TextIO.read().from(place));

    PCollection<String> Link_Lines = Lines.apply(Filter.by((String line) -> line.startsWith("[")));
    PCollection<String> pages = Link_Lines.apply(MapElements.into(TypeDescriptors.strings())
        .via(
            (String linkline) -> linkline.substring(linkline.indexOf("(") + 1, linkline.length() - 1)));
    PCollection<KV<String, String>> kvdata = pages.apply(MapElements
        .into(
            TypeDescriptors.kvs(
                TypeDescriptors.strings(), TypeDescriptors.strings()))
        .via(output -> KV.of(dataFile, output)));
    return kvdata;

  }


}
