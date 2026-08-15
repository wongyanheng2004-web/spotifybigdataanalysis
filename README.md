# Analyzing the Relationship Between Audio Features and Track Popularity Across Genres Using Apache Spark

**IST3134 Group Assignment — May Semester 2026**

## Team

- **Wong Yan Heng** — 22042394
- **Chan Yue Yan** — 22087845

## Project Overview

This project analyses Spotify music tracks to investigate how audio characteristics vary across music genres and how these characteristics are associated with track popularity.

The analysis focuses on:

- Danceability
- Energy
- Valence
- Tempo
- Loudness
- Popularity
- Track genre

The project uses **PySpark** as the Big Data Analytics implementation and **Pandas** as a non-distributed comparison approach.

## Problem

Streaming platforms make a long-standing industry assumption testable: that commercial success can be traced to the measurable acoustic properties of a recording. This project examines whether that assumption holds across a large catalogue.

The project investigates:

1. Which genres have the highest average popularity?
2. How do audio characteristics vary across genres?
3. Which selected audio features have the strongest correlation with popularity?
4. How do high-popularity and lower-popularity tracks differ?
5. Which explains more variation in popularity — genre, or individual audio features?
6. How does PySpark compare with Pandas for a dataset of this size?

## Dataset

The project uses the **Spotify Tracks Dataset** from Kaggle, containing approximately **114,000 Spotify tracks and 21 attributes** across **114 genre labels** (exactly 1,000 records per genre).

Important attributes used in the analysis:

`track_id`, `artists`, `track_name`, `popularity`, `danceability`, `energy`, `loudness`, `valence`, `tempo`, `track_genre`

**Note on `popularity`:** this field is computed by a Spotify algorithm based mainly on total play count and how recent those plays are. It measures listener engagement, not musical quality — a distinction that turns out to be central to the results below.

### Dataset Source

https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset

Download the CSV from Kaggle and place it beside the notebook (or upload it to Google Colab) as `dataset.csv` before running.

## Technologies Used

- Python
- PySpark
- Pandas
- Matplotlib
- NumPy
- Google Colab

## Big Data Approach

PySpark performs the main Big Data analysis. The core analytical operation is a genre-level grouped aggregation, implemented two ways.

**1. Spark DataFrame API**

```python
df_clean.groupBy("track_genre").agg(avg("popularity"), count("*"), ...)
```

**2. Explicit RDD MapReduce**

```python
df_clean.select("track_genre", "popularity").rdd \
    .map(lambda row: (row["track_genre"], (row["popularity"], 1))) \
    .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1])) \
    .mapValues(lambda v: v[0] / v[1])
```

Both express the same **Map → Shuffle → Reduce** process:

- **Map:** each record emits a key-value pair keyed by genre, e.g. `("pop-film", (59, 1))`.
- **Shuffle:** records sharing a key are brought together, so all 1,000 pop-film records land on the same reducer.
- **Reduce:** partial sums and counts are folded together, then divided to give the average.

This shape parallelises because each mapper is independent and the reduce operation is associative and commutative, so partial aggregates can be combined in any order across machines.

Both implementations produce identical genre-level results, confirming the aggregation logic is correct.

## Data Cleaning

| Step | Records |
|---|---|
| Original dataset | 114,000 |
| After cleaning | **113,842** (158 removed) |
| Unique tracks after cleaning | **89,583** |

Removed: 157 records with `tempo = 0`, 1 record with `duration_ms = 0`, and 1 record with missing artist/album/track name.

**Duplicate handling.** The raw file contains only 89,741 distinct `track_id` values; 24,259 rows are repeats, because a track associated with several genres appears once per label. These are a genuine feature of the source data, so the treatment differs by analysis stage:

- **Genre-level aggregation** uses the full cleaned dataset, since each genre label is valid information.
- **Track-level statistics** (correlation, percentile split) use the deduplicated dataset, so no track is counted more than once.

## Analysis Performed

### 1. Data Quality Analysis

Record and column counts, schema, missing values, duplicate `track_id` values, and physically invalid numerical values.

### 2. Genre-Level Analysis

Average popularity, danceability, energy, valence, tempo, loudness and track count for each `track_genre`.

### 3. Correlation Analysis

Pearson correlation between each audio feature and popularity, computed in a single Spark pass using `VectorAssembler` and `Correlation.corr`.

### 4. Genre Variance Analysis

Eta-squared, measuring the proportion of variance in popularity attributable to genre, for comparison against the variance explained by individual audio features.

### 5. High- and Low-Popularity Analysis

Tracks split at the **75th-percentile popularity threshold**, then compared on danceability, energy and valence.

### 6. PySpark vs Pandas Comparison

The same analytical workload implemented in Pandas. Computation times compared with Spark session startup excluded.

| Implementation | Computation Time |
| -------------- | ---------------- |
| PySpark        | 29.16 seconds    |
| Pandas         | 0.09 seconds     |

Spark session startup, measured separately on a freshly restarted runtime, took a further **13.51 seconds** — roughly 150 times longer than Pandas needed to complete the entire analysis.

Both implementations processed the same 113,842 cleaned records and 89,583 unique tracks, and produced **identical correlation coefficients to four decimal places** — so the runtime difference reflects execution behaviour, not different work.

The gap is concentrated in operations that redistribute data between partitions. Removing duplicate track identifiers is the largest single contributor, because Spark performs it as a full shuffle while Pandas performs it as an in-memory hash operation. Spark is not doing the same work inefficiently; it is doing **additional** work — partitioning, shuffling, fault-tolerant coordination — that exists to support datasets too large for one machine. On a 20 MB dataset that cost buys nothing.

## Key Results

- **114,000** records processed, **113,842** retained after cleaning, **89,583** unique tracks.
- **24,259 repeated `track_id` rows** identified; retained for genre analysis, removed for track-level analysis.
- **pop-film** had the highest average popularity at **59.28**; the lowest genres fall to low single digits.
- The 75th-percentile popularity threshold was **49.00**.
- **Loudness** had the strongest correlation with popularity at **+0.0733** — roughly **0.54%** of variance.
- All five audio features showed only negligible linear relationships with popularity.
- **Genre explained approximately 32.59% of the variance in popularity** — around sixty times more than the strongest individual audio feature.
- **Pandas was faster** than PySpark for this dataset, by a factor of approximately 311.
- **Spark session startup alone took 13.51 seconds**, a fixed cost independent of data volume.

**Main finding:** how a track sounds explains very little about how widely it is played; the category it occupies explains a great deal. Since genre does not itself cause listening behaviour, it most plausibly acts as a proxy for distribution factors the dataset does not record — playlist placement, promotion, audience size and regional reach.

**Known limitation:** the global correlations pool 114 genres and mask within-genre variation; the loudness relationship ranges from about **−0.41 to +0.46** depending on genre.

## Visualisations

1. Top 10 genres by average popularity
2. Correlation of audio features with popularity
3. High- versus low-popularity audio features
4. PySpark versus Pandas computation time
5. PySpark startup versus computation time
6. Danceability versus energy across the top genres

All charts are saved to the `output/` directory when the notebook runs.

## Repository Contents

```
spotifybigdataanalysis/
│
├── README.md
├── IST3134_Group_Assignment.ipynb
├── report.docx
└── output/
    ├── chart1_top_genres.png
    ├── chart2_correlations.png
    ├── chart3_high_low_popularity.png
    ├── chart4_runtime_comparison.png
    ├── chart5_spark_startup.png
    ├── chart6_danceability_energy.png
    ├── correlations.txt
    ├── correlations_pandas.txt
    ├── genre_variance.txt
    ├── genre_profile_pandas.csv
    ├── pandas_runtime.txt
    ├── runtime_comparison.txt
    ├── genre_profile/           # Spark CSV output
    ├── mapreduce_genre/         # Spark CSV output (RDD implementation)
    └── missing_values_spark/    # Spark CSV output
```

The notebook also writes a full cleaned copy of the dataset to
`output/cleaned_dataset/`. This is an intermediate file of roughly 21 MB that
can be regenerated by re-running the notebook, so it is not committed here.

## How to Run

The notebook was developed and tested using **Google Colab**.

1. Open `IST3134_Group_Assignment.ipynb` in Google Colab.
2. Download the dataset from the Kaggle link above and upload it as `dataset.csv`.
3. **Runtime → Restart runtime**, then **Run all**, from the first cell to the last.
4. The notebook runs the PySpark analysis, the RDD MapReduce implementation, the Pandas comparison, the runtime benchmark and all visualisations.
5. Results are printed inline and written to the `output/` directory; the Final Summary section collects the main figures.

> Restarting the runtime before running matters: Spark session startup is only measured correctly on a fresh runtime. Re-running cells in an existing session reports a startup time near zero.

## Source Code

Complete source code: `IST3134_Group_Assignment.ipynb`

## Dataset Link

https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
