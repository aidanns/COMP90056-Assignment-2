# Streams Assignment 2 - 2013

This is an implementation of the second project for COMP90057 Stream Processing
from The University of Melbourne, semester 2 2013.

# Author

Aidan Nagorcka-Smith
aidann@student.unimelb.edu.au

# Getting started

## Prerequisites

First, you need `java`, `mvn` and `git` installed and in your user's `PATH`.  

## Configuration

Create a file `conf/twitter.properties` and fill it with the following content,
specifying the OAuth credentials for the twitter account for API access.

    oauth.consumerKey=********************
    oauth.consumerSecret=********************
    oauth.accessToken=********************
    oauth.accessTokenSecret=********************

# Running with Maven

## Install Maven
Install Maven (preferably version 3.x) by following the 
[Maven installation instructions](http://maven.apache.org/download.cgi).

## Running topologies with Maven

To compile and run in local mode, use the command:

    $ mvn compile exec:java -Dexec.classpathScope=compile -Dexec.mainClass=com.aidanns.streams.assignment.two.Topology


## Packaging for use on a Storm cluster

You can package a jar suitable for submitting to a Storm cluster with the command:

    $ mvn package

This will package your code and all the non-Storm dependencies into a single 
"uberjar" at the path 
`target/aidanns-assignment-2-{version}-jar-with-dependencies.jar`.


## Running unit tests

Use the following Maven command to run the unit tests.

    $ mvn test
