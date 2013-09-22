# Streams Assignment 2 - 2013

My solution to COMP90056 Stream Computing and Applications Assignment 2,
Semester 2 2013 at The University of Melbourne.

# Author

* Aidan Nagorcka-Smith
* aidann@student.unimelb.edu.au

# Getting started

## Prerequisites

First, you need `java`, `mvn` and `git` installed and in your user's `PATH`.  

## Configuration

### Twitter credentials

Create a file `conf/twitter.properties` and fill it with the following content,
specifying the OAuth credentials for the twitter account for API access.

    oauth.consumerKey=********************
    oauth.consumerSecret=********************
    oauth.accessToken=********************
    oauth.accessTokenSecret=********************

### Stop words

Stop words (which may not appear in the top words list) are listed in the file `conf/stop_words.txt`, one word per line.

### Input tweets file

Create a file `conf/input.properties` and fill it with the following content,
specifying the name of the file to read tweets from. Make sure this file is in
the `input` directory prior to running the program. Be aware that this file
must not clash in names with another file on the classpath.

    input.tweets.filename=****************

The input file should contain tweets in JSON format, one tweet per line.

# Running with Maven

## Install Maven
Install Maven (preferably version 3.x) by following the 
[Maven installation instructions](http://maven.apache.org/download.cgi).

## Running topologies with Maven

To compile and run in local mode, using the twitter sample stream as the source, use the command:

    $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.aidanns.streams.assignment.two.topology.AssignmentTwoFromStream

To compile and run in local mode, using the local file as the source, use the command:

    $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.aidanns.streams.assignment.two.topology.AssignmentTwoFromFile

## Packaging for use on a Storm cluster

You can package a jar suitable for submitting to a Storm cluster with the command:

    $ mvn package

This will package your code and all the non-Storm dependencies into a single 
"uberjar" at the path 
`target/aidanns-assignment-2-{version}-jar-with-dependencies.jar`.


## Running unit tests

Use the following Maven command to run the unit tests.

    $ mvn test

# Output

Output files are created in the `output/` folder and updated with data from the
running system.

## Similarity

Shows users that have similarity scores calculated > 0.65 based on their 
tweets in a 5 minute sliding window according to the algorithm described in the 
assignment specification. Updated every second.

## Throughput

Shows information on the current throughput of tweets, total number of tweets
processed and time the system has run for, updated every second.
Updated every second.

## Users

Shows the users that send the most tweets using the SpaceSaving algorithm.
Updated every second.

## Words

Shows the most common words or hashtags (configurable in the topology) that
are seen in tweets that are processed. Output is saved every 5 minutes, files
are marked with the number of minutes since the start of the run when they
were created. The latest information is in the file without a suffix, updated
every second..
