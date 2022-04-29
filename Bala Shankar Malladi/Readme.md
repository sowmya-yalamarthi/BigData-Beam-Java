# Java-word-count-beam

Example has been taken from this link :

<https://beam.apache.org/get-started/quickstart-java>

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
mvn compile exec:java -D exec.mainClass=edu.nwmissouri.bigdata.java.group3.balu.MinimalPageRankBalu

```

## Links:

* Group repo link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java
* Readme link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/edit/main/Bala%20Shankar%20Malladi/Readme.md
* Working folder link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/tree/main/Bala%20Shankar%20Malladi
* Wiki link :  https://github.com/sowmya-yalamarthi/BigData-Beam-Java/wiki/Bala-shankar-Malladi
* commits link : https://github.com/sowmya-yalamarthi/BigData-Beam-Java/commits/main
