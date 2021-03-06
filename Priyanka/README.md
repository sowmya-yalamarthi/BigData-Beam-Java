#word-count-beam

Java Quickstart for Apache Beam

<https://beam.apache.org/get-started/quickstart-java>

## Set up Environment

- Java
- Maven

## Get the Sample Project

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

## Execute using DirectRunner

```PowerShell
mvn compile exec:java -D exec.mainClass=org.apache.beam.examples.WordCount `
 -D exec.args="--inputFile=sample.txt --output=counts" -P direct-runner
```

## Execute PR Quick Start

```PowerShell
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.group02.Maruthi.MinimalPageRankMaruthi

mvn compile exec:java -D exec.mainClass=edu.nwmissouri.bigdata.java.grp03_maruthi.MinimalPageRankMaruthi
```

## Links:

* Group repo link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java
* Readme link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/blob/main/Priyanka/README.md
* Working folder link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/tree/main/Priyanka
* Wiki link :  https://github.com/sowmya-yalamarthi/BigData-Beam-Java/wiki/Priyanka-Maruthi 
* commits link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/commits/main
