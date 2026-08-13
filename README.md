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

The large amount of music data available from streaming platforms provides an opportunity to analyse track characteristics systematically across different genres.

This project investigates:

1. Which genres have the highest average popularity?
2. How do audio characteristics vary across genres?
3. Which selected audio features have the strongest correlation with popularity?
4. How do high-popularity and lower-popularity tracks differ?
5. How does PySpark compare with Pandas for the selected dataset?

## Dataset

The project uses the **Spotify Tracks Dataset** from Kaggle.

The dataset used in the analysis contains approximately **114,000 Spotify tracks and 21 attributes**.

Important attributes used in the analysis include:

- `track_id`
- `artists`
- `track_name`
- `popularity`
- `danceability`
- `energy`
- `loudness`
- `valence`
- `tempo`
- `track_genre`

### Dataset Source

https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset

The dataset should be downloaded from Kaggle and uploaded to Google Colab as `dataset.csv` before running the notebook.

## Technologies Used

- Python
- PySpark
- Pandas
- Matplotlib
- NumPy
- Google Colab

## Big Data Approach

PySpark is used to perform the main Big Data analysis.

The main analytical operation is:

```python
df.groupBy("track_genre").agg(...)
```

The operation can be explained using the conceptual **Map → Shuffle/Group → Reduce** process:

- **Map:** Each Spotify record is associated with its `track_genre` key and relevant numerical values.
- **Shuffle/Group:** Records belonging to the same genre are grouped together.
- **Reduce:** Aggregate functions calculate average popularity, average audio features and track counts for each genre.

This provides a Spark-based approach for summarising the Spotify dataset by genre.

## Analysis Performed

### 1. Dataset and Data Quality Analysis

The project checks:

- Number of records
- Number of columns
- Dataset schema
- Missing values
- Repeated `track_id` values

### 2. Genre-Level Analysis

The PySpark implementation calculates:

- Average popularity
- Average danceability
- Average energy
- Average valence
- Average tempo
- Track count

for each `track_genre`.

### 3. Correlation Analysis

The project calculates the correlation between the following audio features and popularity:

- Danceability
- Energy
- Valence
- Tempo
- Loudness

### 4. High- and Low-Popularity Analysis

Tracks are divided using the **75th-percentile popularity threshold**:

- **High popularity:** popularity greater than or equal to the 75th percentile
- **Low popularity:** popularity below the 75th percentile

The average danceability, energy and valence of the two groups are then compared.

### 5. PySpark vs Pandas Comparison

The same major analytical tasks are implemented using Pandas as the comparison approach.

The computation times are compared after excluding Spark session startup time.

For the executed 114,000-record dataset, the recorded result was:

| Implementation | Computation Time |
|---|---:|
| PySpark | 7.53 seconds |
| Pandas | 0.19 seconds |

Both implementations processed **114,000 records**, and the consistency check confirmed that the row counts were the same.

Pandas was faster for this dataset because the dataset can be processed efficiently on a single machine, while PySpark introduces additional processing overhead. This runtime comparison is specific to the selected dataset and execution environment.

## Key Results

The executed analysis produced the following main findings:

- **114,000** Spotify track records were processed.
- No missing values were found in the main analytical columns checked.
- **24,259 repeated `track_id` rows beyond the first occurrence** were identified and retained for analysis.
- **pop-film** had the highest average popularity, with an average score of **59.28**.
- The 75th-percentile popularity threshold was **50**.
- **Loudness** had the strongest absolute correlation with popularity at approximately **+0.0504**.
- The selected audio features showed only very weak linear relationships with popularity.
- **Pandas was faster** than PySpark for the executed 114,000-record dataset.

## Visualisations

The notebook produces visualisations for:

1. Top 10 genres by average popularity
2. Correlation of audio features with popularity
3. High- versus low-popularity audio features
4. PySpark versus Pandas computation time
5. PySpark startup versus computation time
6. Danceability versus energy across the top genres

## Repository Contents

```text
spotify-bigdata-analysis/
│
├── README.md
├── IST3134_Group_Assignment.ipynb
└── report.docx
```

## How to Run

The notebook was developed and tested using **Google Colab**.

1. Open `IST3134_Group_Assignment.ipynb` in Google Colab.
2. Upload the Kaggle CSV dataset as `dataset.csv`.
3. Run the notebook from the first cell to the last cell.
4. The notebook performs the PySpark analysis, Pandas comparison, runtime comparison and visualisations.
5. The final Key Findings section summarises the main results.

## Source Code

The complete source code is provided in:

`IST3134_Group_Assignment.ipynb`

## Dataset Link

https://www.kaggle.com/datasets/maharshipandya/-spotify-tracks-dataset
