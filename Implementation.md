# Implementation

This document describes the implementation of the assignment.

## Program Structure

The topology is a simple fan, sourcing tweets from either a file or the twitter
streaming API and piping them in to the application through a spout. All bolts
read statuses directly from the spout and do processing on them.


 [Source]--- [Throughput Recorder]
          |
          |- [Top 20 Words]
          |
          |- [Top 5 Users]
          |
          |- [User Similarity]

## Filtering

Filtering is done at the source level using the Twitter API. Only Tweets marked
as English are returned.

## Throughput

`src/java/com/aidanns/streams/assignment/two/bolt/StatusThroughputRecorderBolt.java`

Throughput is recorded using simple counters.

## Top 20 Words

`src/java/com/aidanns/streams/assignment/two/bolt/TopKWordsBolt.java`

The top 20 words are recorded using the SpaceSaving algorithm with storage
space for 1000 items. This number was chosen by inspection to provide enough 
space that the counts for the top 20 words can be recorded acurately.

## Top 5 Users

`src/java/com/aidanns/streams/assignment/two/bolt/TopKUsersBolt.java`

The top 5 users are recorded using the SpaceSaving algorithm with storage space
for 5000 items. This number was chosen by inspection to provide enough space
that the counts for the top 5 users can be recorded accurately.

## User Similarity

`src/java/com/aidanns/streams/assignment/two/bolt/UserComparisonBolt.java`

The user similarity is calculated over a 5 minutes sliding window of tweets.
Only similarities that could have changes (because a tweet was added or removed
for that user) are re-calculated when a window is processed, to save compute
resources. The window is re-evaluated after every status arrives on the bolt.

# Ouputs

The outputs from a 1 hour run against the Twitter stream API can be found 
at `sample-output`.

# Conclusions

The most important conclusions that I drew from the subject were how the type of
stream relates to the algorithms that are chosen. I originally used much smaller
ammounts of space for the SpaceSaving algorithms and was unable to count
accurately. As the frequencies within the stream get smaller, more space needs
to be allocated to get accurate results. Algorithms must suit the requriements
of the application.