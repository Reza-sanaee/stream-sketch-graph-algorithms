# RDD Streaming, Cardinality Sketches, and PageRank Algorithms

A DStream/RDD laboratory replays traffic files, computes sliding-window counts/diversity, estimates distinct vehicles with four-hash FM-style sketches, and constructs a camera transition graph with iterative PageRank. The rewrite corrects sketch results and dangling-node probability loss, and recomputes the small theoretical graph.

## Technical highlights

- Implemented Spark DStream/RDD traffic-window analytics and four-hash cardinality estimation with exact-count comparisons.
- Built camera-transition graphs and iterative PageRank; analyzed DGIM, Bloom-filter, HITS, and ranking/sketch correctness.

## Explore the project

- [Source and execution guide](code/RUNNING.md)
- [Technical report (PDF)](report/Report_EN.pdf)
- [Technical report (Markdown)](report/Report_EN.md)
- [Assignment/specification (PDF)](assignment/Assignment_EN.pdf)

## Reproduction and attribution

Read the execution guide and report before running the code. Dependencies, input data, licensed tools, and hardware requirements vary by project. Recorded results are distinguished from rerun results in the accompanying documentation. Team and third-party source attribution in the reports and source headers is retained. This portfolio does not claim independent authorship of supplied frameworks or dependencies.

## Input data boundary

See [public delivery and input data](DATA_PROVENANCE.md). Original stream-input batches are not publicly redistributed.
