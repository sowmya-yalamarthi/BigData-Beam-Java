package nwm.apache.beam.projects;
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
//imported a PcollectionList, Pcollection and TypeDescription
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.TypeDescriptor;
// created a class name MinimalPageRankTejaswi
public class MinimalPageRankTejaswi {
  public static void main(String[] args) {
    //created a PipelineOptions object
    PipelineOptions options = PipelineOptionsFactory.create();
    //created a PipelineOptions object that has been defined above
    Pipeline t = Pipeline.create(options);
    //created a datafolder where web04 file is being used
    String dataFolder = "web04";
   //generated a key,value pairs for each webpage
   PCollection<KV<String,String>> ta1 = TejaswiMapper01(t,"go.md",dataFolder);
   PCollection<KV<String,String>> ta2 = TejaswiMapper01(t,"python.md",dataFolder);
   PCollection<KV<String,String>> ta3 = TejaswiMapper01(t,"java.md",dataFolder);
   PCollection<KV<String,String>> ta4 = TejaswiMapper01(t,"README.md",dataFolder);
   //merged all the key value pairs in to the PCollectionList and then in to the Pcollection
    PCollectionList<KV<String, String>> pCollectionList = PCollectionList.of(ta1).and(ta2).and(ta3).and(ta4);
    PCollection<KV<String, String>> mergedList = pCollectionList.apply(Flatten.<KV<String,String>>pCollections());
    PCollection<String> pLinksString = mergedList.apply(MapElements.into(TypeDescriptors.strings()).via((mergeOut)->mergeOut.toString()));
    // generating the output with the file name tejaswicounts
    pLinksString.apply(TextIO.write().to("tejaswicounts"));  
    t.run().waitUntilFinish();
  }
 public static PCollection<KV<String,String>>TejaswiMapper01(Pipeline t, String filename, String dataFolder){
    String newdataPath = dataFolder + "/" + filename;
     PCollection<String> pcolInput = t.apply(TextIO.read().from(newdataPath));
     PCollection<String> pcollinkLines = pcolInput.apply(Filter.by((String line) -> line.startsWith("[")));
     PCollection<String> pcolLinks = pcollinkLines.apply(MapElements.into((TypeDescriptors.strings()))
     .via((String linkLine) ->linkLine.substring(linkLine.indexOf("(")+1, linkLine.length()-1)));
     PCollection<KV<String,String>> pColKVPairs =  pcolLinks.apply(MapElements.into(TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.strings()))
     .via((String outLink) -> KV.of(filename,outLink)));
    return pColKVPairs;
  }
 

}
 