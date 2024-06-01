# Giannis Pierropoulos' Scala Projects

Welcome to my GitHub repository! This repository contains Scala solutions to various data processing and analysis tasks. Each task is represented by a Scala file corresponding to a specific question. Below you'll find descriptions of each task along with the methodology and outputs.

## Table of Contents
- [Questions](#questions)
  - [Question 1](#question-1)
  - [Question 2](#question-2)
  - [Question 3](#question-3)
  - [Question 4a](#question-4a)
  - [Question 4b](#question-4b)
- [Outputs](#outputs)

## Questions

### Question 1
**Task:** Calculate the average length of words starting with a specific character from a text file.

**Methodology:**
1. Initialize Spark context.
2. Read the text file and clean the text by removing punctuation and converting to lower case.
3. Split the text into words, filter those starting with a specific letter, and create key-value pairs with the character as the key and the word length as the value.
4. Compute the average word length for each character.
5. Sort and display the results.

### Question 2
**Task:** Analyze tweets to find:
1. The 5 most frequent words in tweets for each sentiment (positive, negative, neutral) related to airlines.
2. The main complaint for each airline.

**Methodology:**
1. Initialize Spark context and read the CSV file into a DataFrame.
2. Clean the text data and add a sentiment column based on confidence levels.
3. For task 1, tokenize the text and count word frequencies for each sentiment.
4. For task 2, group tweets by airline and complaint reason to find the most common complaints.

### Question 3
**Task:** Perform various analyses on a dataset of movies.
1. Count the number of movies per genre.
2. Find the top 10 years with the most movie releases.
3. Identify the most common words in movie titles with a length of 4 or more characters.

**Methodology:**
1. Use Spark DataFrame operations to manipulate and analyze the dataset.
2. Extract genres, years, and words from titles using regular expressions and aggregations.

### Question 4a
**Task:** Analyze an edge list to find the top 10 nodes by incoming and outgoing edges.

**Methodology:**
1. Load the edge list and filter out comment lines.
2. Compute the number of incoming and outgoing edges for each node.
3. Identify and display the top 10 nodes with the most incoming and outgoing edges.

### Question 4b
**Task:** Compute the degree of each node and find nodes with a degree greater than or equal to the average degree.

**Methodology:**
1. Calculate the total degree (sum of incoming and outgoing edges) for each node.
2. Compute the average degree and filter nodes with a degree above this value.
3. Display the count of nodes and some random nodes with a degree above the average.

## Outputs

### Question 1 Output:
```bash
(c,7.188253044980429)
(e,7.1251542321296375)
(q,7.01324200913242)
...
(o,3.0125874667561776)
```


### Question 2 Output:
**Top 5 words for positive sentiment:**
```bash
+------+-----+
| word|count|
+------+-----+
| the| 903|
| to| 880|
...
```
**Main complaints for each airline:**
```bash
+--------------+--------------------+
| airline| main_complaint|
+--------------+--------------------+
| Delta| Late Flight|
|Virgin America|Customer Service ...|
...
```


### Question 3 Output:
**Movies per genre:**
```bash
+------------------+------+
| genre|movies|
+------------------+------+
|(no genres listed)| 5062|
| Action| 7348|
...
```
**Movies per year:**
```bash
+----+------+
|year|movies|
+----+------+
|2015| 2513|
|2016| 2488|
...
```

### Question 4a Output:
**Top nodes by edges:**
```bash
Top 10 nodes by incoming edges:
226411 -> 38606
234704 -> 21920
...
```

### Question 4b Output:
**Nodes with grade above average:**
```bash
Number of nodes with grade >= 17.024891360896042: 508
...
```

