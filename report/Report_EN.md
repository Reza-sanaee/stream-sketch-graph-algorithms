## Theory 1: graph model and transition convention

The graph has A -> B,C,D; B -> A,D; C -> A; D -> B,C. With destination rows and source columns in order A,B,C,D, the column-stochastic matrix is:

| Destination/source | A | B | C | D |
|---|---:|---:|---:|---:|
| A | 0 | 1/2 | 1 | 0 |
| B | 1/3 | 0 | 0 | 1/2 |
| C | 1/3 | 0 | 0 | 1/2 |
| D | 1/3 | 1/2 | 0 | 0 |

Solve r=0.85 M r+0.15 e with the appropriate normalized teleport distribution. B/C/D have equal stationary scores in some of these teleport configurations despite having different outgoing-link structures. The stationary numerical vectors are provided in the numerical calculations section below. Focused teleport favors its seed directly and influences other nodes through the actual graph transitions. TrustRank uses e=(0,1,0,0). Raw spam mass is 1-TR/PR and can be negative for trusted pages. Clipping negative values to zero is an optional display convention and must be stated. HITS authority and hub vectors are eigenvectors associated with incoming/outgoing good-link structure and depend on normalization. PageRank describes a different random-walk model; their orderings need not agree. 

## Theory 2: complete DGIM bucket trace

Process the sixteen-bit sequence in timestamp order, storing at most two buckets per power-of-two size and merging the two oldest equal-size buckets. A bucket timestamp is its newest represented one, not the start of its interval. At t=12 the buckets newest-first are (1,12),(2,11),(2,8),(4,5). At t=13 add (1,13), leaving two unit buckets. t=14 is zero. At t=15 add (1,15); merge the older unit buckets into (2,13), then merge the older size-two buckets into (4,11). At t=16 add (1,16).

| Final bucket size | Timestamp | Represented one timestamps |
|---|---:|---|
| 1 | 16 | 16 |
| 1 | 15 | 15 |
| 2 | 13 | 12,13 |
| 4 | 11 | 7,8,10,11 |
| 4 | 5 | 1,3,4,5 |

For N=12, timestamp 5 is still allowed at t=16, although that oldest bucket includes earlier ones. For K=7 the query covers t=10..16. Sum the fully included newest buckets and half the boundary bucket: 1+1+2+4/2=6. The exact last-seven count is also six, giving absolute error zero and relative error zero. Exact agreement in this example does not make DGIM an exact counter in general.

## Theory 3: FM maximum register

The six hash strings have trailing-zero counts 3,5,4,3,3,6; R=6 and the simple estimate is 64. A nine-bit all-zero string gives a capped trailing-zero count of 9 and estimate 512 under the exercise convention. A finite-width implementation must explicitly handle zero hashes instead of looping indefinitely. One unusually long zero suffix can double the estimate for every additional register bit. Therefore the estimate can overshoot or undershoot the true count, and adding one element need not reduce error. Uniform hashes give a suffix probability of about 2^(-k), explaining the noisy maximum statistic.

## Theory 4: Bloom-filter derivation

Each partition has n/k bits and receives one hash per inserted element. A selected bit remains zero with probability (1-k/n)^m, approximately exp(-mk/n). Under independent partitions, false-positive probability is [1-(1-k/n)^m]^k, approximately (1-exp(-mk/n))^k. A conventional single-array filter has a closely related approximation, not an exactly identical finite-size occupancy model. Minimize the log of the approximation in continuous k to obtain k=(n/m) ln 2; choose a nearby positive integer and require valid partition sizes. Independence and uniformity are modeling assumptions.

## Theory 5: cooperating spam farms

The submission proposes mutual target-page links while support pages point to their own target. A mutual link can retain or redirect some probability within the combined farm. However each new outgoing link also divides the source target's outgoing probability, and benefits depend on support links, external inflow, and the recorded farm model. Without those constraints, the proposed connection is a candidate strategy, not a proved universal PageRank-maximizing topology. 

## Practical 1: file-based stream simulation

A Python feeder reads traffic.csv in chunks of 5000 records and publishes a new CSV file every five seconds. Spark's textFileStream watches the input directory and detects newly created files. Files should be written completely and atomically renamed into the watched directory so that a partial file is not consumed. Replaying a static dataset creates a simulated arrival stream, not live camera telemetry. The StreamingContext uses a five-second batch interval. Parsing extracts camera from index 0, passage time from index 4, and plate from index 6. Console samples demonstrate newly arriving tuples in successive batches. Splitting on commas assumes simple unquoted fields; a CSV-aware parser is needed when fields may contain quoted commas. The screenshots document this classroom run without establishing exactly-once delivery or restart recovery.

## Volume windows and top-three cameras

A thirty-second window advances every ten seconds, covering six five-second batches when full. At the configured feeder rate, a full window can contain about 30000 records. Early counts rise while the window fills; later counts reflect replay-arrival time, not necessarily a real thirty-second interval in the recorded timestamps. reduceByKey obtains counts by camera, and sorting selects the top three. The submitted examples repeatedly identify cameras 900212 and 100700853 among high-count cameras, with sample counts rising from about 200 toward 400 as the window fills. These counts should not be presented as a measured physical camera flow rate because the feeder compresses/replays the data. A larger window smooths fluctuations but retains older arrivals; a smaller window reacts faster but is noisier. A thirty-second window does not guarantee a thirty-second maximum detection delay because triggers, backlog, and processing also contribute. Ranking agreement with an earlier static analysis is a consistency observation, not an independent correctness proof.

## Repeated plates and behavioral interpretation

The behavioral query uses a sixty-second window, a ten-second slide, and threshold N=3: plates with at least four recorded observations qualify. N is a parameter. Longer windows capture more repeated observations but require more distinct-key state and can combine unrelated events. Shorter windows can separate occurrences that a longer interval would group. Repeated plate observations do not prove multiple distinct camera visits, speeding, prohibited travel, or suspicious intent. Those interpretations require validated identities, locations, travel times, and camera deduplication. The recorded physical-distance explanation is especially inappropriate when timestamps are replayed at a different wall-clock rate.

## Camera diversity

A sixty-second window with a twenty-second slide computes total observations per camera and distinct (camera,plate) pairs. Joining these counts gives diversity=UniquePlates/TotalPasses, between zero and one for nonempty windows. A low ratio indicates repeat observations, which may reflect congestion, duplicate detections, loops, parking movements, or sensor behavior. It does not uniquely identify a road type. The report filters TotalPasses>100 and diversity<0.6. Its example camera 100701262 has 155 observations and 33 distinct plates: 33/155=0.2129032, or about 4.697 observations per distinct plate. A low-diversity camera is not proven to be outside a highway. Changing window length changes which repeats are grouped; more data can help interpretation while also reducing temporal specificity. 

## Practical 2: four-hash distinct counting

camera_passes.csv supplies CameraID and PlateNumber. Each plate is hashed with md5, sha1, sha224, and sha256; the maximum trailing-zero register for each hash is maintained. The report combines the four registers by their median and uses 2 raised to that median as its estimate. This reduces the influence of one exceptionally large register, but four hashes still have high variance. If the median of an even number of registers is their middle-pair average, its value can be half-integer and the estimate need not be an integer power of two. 

| Submitted sample | Exact distinct | Reported estimate | Absolute relative error |
|---|---:|---:|---:|
| Global sample 1 | 1793 | 2896 | 61.5170% |
| Global sample 2 | 3318 | 4870 | 46.7752% |

The exact reference is distinct().count(). Four registers provide a small sketch, while the exact reference requires work/state proportional to observed distinct items. The errors illustrate the weakness of this simple estimator; they are not a demonstrated accuracy guarantee or an inevitable fixed error range. The pipeline's exact-evaluation operations also consume resources, so the complete experiment cannot be described as constant-memory merely because the register summary is small. For expiring windows, a cumulative maximum register cannot simply forget old items. Recomputing a sketch for each window or using an explicitly window-aware structure is necessary. This distinction should be checked against the notebook before interpreting a cumulative result as a sliding-window estimate.

## Practical 3: directed camera-transition graph

Observations are grouped by plate and sorted by recorded time. Each consecutive pair with different cameras produces an observed directed transition; repeated observations at one camera are removed as self-transitions in the described design. The report states 50 nodes and 79310 edges. For a simple directed graph with 50 nodes and no loops, at most 2450 unique directed camera pairs exist. Consequently 79310 refers to transition events or parallel edges, not unique camera-pair edges. Preserve that distinction when computing degree, weights, or drawing a simple graph. Observed transitions do not reconstruct every intervening road segment.

## PageRank with dangling nodes

Ranks start at 1/N and use beta=0.85 for ten iterations. All nodes are retained through rightOuterJoin. Each iteration computes dangling mass d=sum(ranks of zero-outdegree nodes) and adds beta*d/N to every node in addition to (1-beta)/N, under uniform redistribution. For weighted transitions, normalize by outgoing weight; for an unweighted graph, normalize by outdegree. The probability-preserving recurrence is r_new(v)=(1-beta)/N+beta*d/N+beta*sum_u r(u)*P(u -> v). Check nonnegativity, rank sum near one, and a residual/convergence criterion. Ten iterations alone do not prove convergence. The traffic-dataset rankings are recorded outputs. The stationary four-node calculation is a separate theoretical example.

## Rankings, count comparison, and visualization

The reported top ten are 48,31,36,24,4,28,29,19,40,20. The traffic-count top ten are 13,39,3,41,22,19,32,48,28,46. Their overlap is 48,28,19. Equal displayed ranks near 0.020109 can result from rounding, dense connectivity, or implementation choices; they do not prove structural symmetry or a main corridor.

| Comparison | Cameras |
|---|---|
| Both lists | 48,28,19 |
| Rank only | 31,36,24,4,29,40,20 |
| Count only | 13,39,3,41,22,32,46 |

A top-15 rank-selected subgraph is drawn with NetworkX/Matplotlib and line thickness based on transition counts. PageRank supplies a structural random-walk score, while observation totals supply volume. Neither score alone establishes a physical bottleneck, predicts disruption severity, or validates route planning. Such conclusions require topology, capacity, ground truth, and intervention evidence. 

## Four-node ranking results

| Model, node order A/B/C/D | A | B | C | D |
|---|---:|---:|---:|---:|
| Uniform PageRank | 0.324561404 | 0.225146199 | 0.225146199 | 0.225146199 |
| Teleport A | 0.403508772 | 0.198830409 | 0.198830409 | 0.198830409 |
| Teleport A/C | 0.373245614 | 0.183918129 | 0.258918129 | 0.183918129 |
| Trust B | 0.284903047 | 0.323453370 | 0.173453370 | 0.218190212 |
| Raw spam mass | 0.122190612 | -0.436637047 | 0.229596719 | 0.030895420 |
| HITS hubs, max normalized | 1.000000000 | 0.391943596 | 0.102775049 | 0.710831454 |
| HITS authority, max normalized | 0.289168546 | 1.000000000 | 1.000000000 | 0.813606503 |

These checks apply to the theoretical four-node graph, not a rerun of the traffic graph.
