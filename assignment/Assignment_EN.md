# Stream sketches and graph ranking: assignment requirements

## Theory exercise 1: ranking a four-page directed graph

For the graph supplied in the fourth Big Data Analytics assignment, write its transition matrix, giving the probability from each page to each outgoing neighbor. Use beta=0.85 when needed. Compute topic-sensitive PageRank with teleport to A alone, then equally to A/C, and explain rank changes. With B as the trusted page, compute TrustRank and Spam Mass and discuss suspicious pages. Compute HITS authority/hub scores and explain whether high authority implies high PageRank. Preserve the original graph figure with these questions.

## Theory exercise 2: DGIM

Process S=1011101101111011 in chronological order with window N=12. Show all bucket merges, final sizes/timestamps, and time coverage. Estimate the number of ones in the last K=7 bits. Compare with the true count and calculate absolute/relative error.

## Theory exercise 3: Flajolet-Martin

For 001011000,100100000,010010000,110111000,000001000,101000000 compute trailing-zero counts and R=max r(a). Estimate distinct cardinality with 2^R. Add 000000000 and compute the new R. Explain why an additional element need not improve the estimate, and discuss under/overestimation when m is much larger/smaller than 2^R.

## Theory exercise 4: partitioned Bloom filter

An allowed-email set has m entries and n available bits. Divide memory into k arrays and hash once into each. Derive false-positive probability as a function of m,n,k, compare to k hashes in one array, and determine the approximate optimal number of hashes. State the assumptions about uniform independent hashing and equal array sizes.

## Theory exercise 5: linked spam farms

Discuss how two cooperating spam farmers could link their farms to increase target-page PageRank. Explain the proposed structure and any advantage, taking account of outdegree dilution and graph assumptions rather than claiming a universal optimum without specifying the farm model.

## Practical exercise 1: DStream traffic windows

Simulate streaming arrival from traffic.csv. Use Spark Streaming DStreams and RDD transformations/actions only; DataFrames and Structured Streaming are prohibited. Choose and justify window/slide durations based on input rate and purpose. Display total passage counts and (DeviceCode,count) by camera for every window. Report the three busiest cameras and explain why windows are needed, the stability/responsiveness tradeoff, and the effect of shorter/longer windows. Choose a behavioral window and adjustable N; report Plate_index values recorded more than N times. Explain the interval and alternatives. For each camera compute distinct plates divided by total records in a selected window. Interpret the ratio cautiously, justify the interval, identify high-count/low-diversity cameras, discuss plausible settings, and explain sensitivity to window length. Deliver complete code, analysis, logs, and console screenshots.

## Practical exercise 2: probabilistic distinct counting

The supplied camera-pass stream contains RecordID, CameraID, PlateNumber, RecordTime, Lane, and Speed. Read PlateNumber as a stream. Use hashlib with md5, sha1, sha224, and sha256, convert hashes to binary, and implement trailing-zero-based FM counting. Estimate distinct plates globally and per camera. Report exact distinct counts, estimates, and preferably percentage error. Keep the distinction between cumulative sketches and expiring windows explicit.

## Practical exercise 3: traffic PageRank

Load passages as an RDD, group each plate's observations, and sort by timestamp. Generate a directed edge from the previous camera to the next for each consecutive passage. Show an adjacency-list sample and count nodes/edges. Implement iterative PageRank with initial 1/N and beta=0.85. Identify and correctly handle nodes without outgoing links. Report the ten highest ranks and compare them with the ten highest traffic counts from the previous exercise. Explain what graph structure adds to raw observation totals. Plot a rank-selected subgraph: cameras as nodes, observed transitions as directed edges, and line thickness proportional to a clearly defined importance/weight. Distinguish event-edge counts from unique camera-pair counts.
