# Prerequisites

- Flink, used version: 1.2.0 (see build.sbt)
- sbt (scala build tool), used versions: 0.13.12, 1.0.0

## Compiling with sbt

To compile the project into a .jar file:

    sbt assembly

This creates a .jar file assembly here (name depends on version set in build.sbt):

    target/scala-2.11/combined-occurrences-assembly-0.1.0.jar

## Compiling with IntelliJ IDEA

To compile and run the project in IntelliJ IDEA, change the flink dependencies from "provided" to "compile" in build.sbt:

    libraryDependencies ++= flinkDependencies.map(_ % "compile"/*"provided"*/)

# TwitterSources

### CSV tweet data set

This is a simple CSV format where each line is a tweet, the first line is a header, and the fields are:
- time: tweet time in unix timestamp
- source_id: twitter account id
- source_name: twitter account name
- language: language as 2 letters
- text: tweet text

# Running

All these programs need Flink running. To run Flink locally:

    /path/to/flink-1.2.0/bin/start-local.sh

You may have to increase the akka framesize if there is an error that akka message size exceeds the limit in Flink config. Put for example "akka.framesize: 209715200b" (default is 10485760b) into the flink config:

    /path/to/flink-1.2.0/conf/flink-conf.yaml

## Running the TweetsWordCount program

This program counts the number of occurrences of all words in the given CSV format tweet data set, and outputs the words in descending count order.

Optional program parameters: (parameter in example)
- minimum word occurrences (6)
- write occurrence count next to words boolean (true)


    /path/to/flink-1.2.0/bin/flink run -c hu.sztaki.ilab.sketches.TweetsWordCount /path/to/combined-occurrences-assembly-0.1.0.jar /path/to/tweets.csv /path/to/words-with-counts.txt 6 true

    cat /path/to/words-with-counts.txt | cut -d' ' -f1 > /path/to/words.txt
    
## Running TwitterCombinedOccurrences

This program stores the given CSV format tweet data set in a probabilistic CountMin sketch data structure, and responds to combined occurrences requests, outputting the words which occur the most often with the word in the request.

Configuration examples:

    src/main/resources/*.conf

WriteStream is an input stream read from the given tweet CSV file set in the config. Use full paths in config!

ReadStream is another input stream read from a socket client listening on the port set in the config. To run a corresponding netcat socket server on e.g. port 15111: 

    nc -lk 15111

Request strings sent to the socket, should consist of 2 strings: the requested word, and window begin time as ISO date. Example:

    rolandgarros 2017-01-01T00:00:00

To run the combined occurrences calculator with specified config:

    /path/to/flink-1.2.0/bin/flink run -c hu.sztaki.ilab.sketches.TwitterCombinedOccurrences target/scala-2.11/combined-occurrences-assembly-0.1.0.jar src/main/resources/conf.conf

The result of the requests coming into the ReadStream socket is written in the readsOutput file (set in config). To see the responses:, e.g.:

    tail -f /path/to/out.txt

To stop, close (Ctrl-C) the socket server.

### Running windowed TwitterCombinedOccurrences

To run the program in windowed mode, set the window parameters in the config: set "windowed" to true, and set windowType (string) and windowParams (list of string).

E.g. to use sliding windows, set windowType to "slidingRanges" and set its 4 parameters in windowParams:
- start time as ISO date string: the start time of the first window
- end time as ISO date string: the last window starts before end time
- size in seconds as string: the size of all windows
- slide in seconds as string: the time between window starting times


