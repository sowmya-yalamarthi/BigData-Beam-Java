## Apache Beam Java


## Get the Sample Project from

<https://beam.apache.org/get-started/quickstart-java>


```PowerShell
mvn archetype:generate `
 -D archetypeGroupId=org.apache.beam `
 -D archetypeArtifactId=beam-sdks-java-maven-archetypes-examples `
 -D archetypeVersion=2.36.0 `
 -D groupId=org.example `
 -D artifactId=word-count-beam `
 -D version="0.1" `
 -D package=org.apache.beam.examples `
 -D interactiveMode=false`
```

## run wordcount using direct runner by following command:

```
mvn compile exec:java -Dexec.mainClass=org.apache.beam.examples.WordCount \
    -Dexec.args="--inputFile=sample.txt --output=counts" -Pdirect-runner
```


To Compile:

```
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.bigdata.java.grp03_sowmya.MinimalPageRankSowmya
```
