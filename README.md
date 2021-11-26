# WDPS 2021 - Assignment 1

This is the project repository for the group assignment 1 in the course Web Data Processing Systems (WDPS) at the Vrije Universiteit, Amsterdam for the academic year 2021-2022 by group 19.
The task of the assignment is to extract named entities from web pages and link them to potential entity candidates in the WikiData knowledge base.

The following is an overview of the functionality provided by our program and of the different steps in our data pipeline:

- Page metadata parsing and raw text-from-HTML extraction
- Natural Language Processing (NLP) pre-processing (tokenization, pos tagging etc.)
- Named entity recognition (NER)
- Candidate ranking and linking

## Setup

We assume that the reader will be running this program inside the `karmaresearch/wdps_assignment1` docker container and to also have the `data` and `assets` folder with the WikiData and Elasticsearch data available.

Make sure that you are in the `/app/assignment` folder and that the project files are in this same folder (we expect you to have the following folder structure).

    /app
        /assignment
            /assets
            /data
            /src
            ...
            README.md
            main.py
            ...
            setup.sh

Then, run the following command:

    ./setup.sh

We are assuming that the Elasticsearch server is already running inside the container, however we provide an option to start it with the setup script. You can run the setup script as following to specify that you want to also start the Elasticsearch server:

    ./setup.sh --start-elasticsearch

WARNING: in the setup, we included the command to start the Elasticsearch server. However, we encountered an error in the Elasticsearch startup when the following command was not run in the host environment before running the setup script:

    sudo sysctl -w vm.max_map_count=262144

After setup is complete and your Elasticsearch instance is up and running (please make sure this is the case as the program requires it) you can run the program in the following way:

    python3 main.py <INPUT_WARC_GZ_ARCHIVE_PATH> > <OUTPUT_TSV_FILE_PATH>

The `INPUT_WARC_GZ_ARCHIVE_PATH` must be an existing `.warc.gz` file path in the docker container.

## Data Preprocessing

Once the WARC records are extracted from the archive, we parse each record's metadata with regular expressions (with the most important piece of information being the “id” to differentiate between extracted records).

Then, we skip the HTTP headers and extract the HTML from the page. The HTML tags are processed using the `beautifulsoup` Python package in order to only output the raw text.

## Entity Recognition

Once the raw text is extracted from the HTML page, we proceed to extract the relevant named entities. To perform this task, two different tools have been considered and applied: the `nltk` Python package (“natural language toolkit” <a href="#ref_1">[1]</a>) and a NER tool integrated in SpaCy <a href="#ref_2">[2]</a>, another Python library.

In the former case, only tokenization, stopword removal, PoS tagging and parsing were performed while SpaCy provides a trained English language model that performs the same tasks and while also providing named entity recognition (NER).

SpaCy additionally labels each entity to provide some contextualization.
We relied on this preliminary labels to restrict our search of candidates in the next phases.

## Candidate Generation and Entity Linking

In order to generate candidates we perform a search based on string similarity through Elasticsearch in order to obtain related documents in the WikiData for the given entity.
Once the response from Elasticsearch is received, a list of candidate objects is created comprising the id, the similarity score (from Elasticsearch) and the available text information.

Several methods have been considered for linking the candidates:

1. On a first experimentation, only the Elasticsearch score was used to rank the candidates which proved to be very inaccurate.

2. The next approach considered, which is now part of our implementation, is to use of the SpaCy entity labels to score the candidates in the list based on context dependent features available by querying the knowledge base (via Trident). For this strategy, we built queries based on pregress knowledge that could use the relationships in the knowledge based graph to produce some notion of 'similarity' score for a given candidate given a label.
   However, on most occasions, the labels obtained from SpaCy are very general and encompass a wide range of possible entities. This implies that it slightly improves the metrics considered in comparison to the first method.
   In some cases, we identify a superclass which accurately maps back to a SpaCy entity label. Nonetheless, quite often, ad-hoc queries were constructed to produce significant scores for the candidates’ “compliance” to such labels. While benefitial, this method introduces human bias in the equation.
   More specifically, manual pattern recognition is performed on various examples obtained by searching through the knowledge base of WikiData, where mostly the properties P31 and P279; “instance-of” and “subclass-of” respectively are leveraged to favour attributes that are present in specific entity types. This increases the possibilities for correct entity linking. Additionally, for each entity class, the patterns identified add further points to each type and the type with the highest score is preferred as it is the most likely type according to the model developed.

3. One last experimentation performed is the **cosine similarity** between the named entity word vectors (also produced through SpaCy) and its candidates. However, this implementation does not perform significantly better and we opt to leave it out of the pipeline (the source code is still present and available to read) as it is more computationally intensive.

## Scalability and Efficiency

Parallelization is essential to make use of all available resources efficiently.
We took a two-sided approach and decided to implement:

- **Multi-processing** for the parallelization of CPU bound tasks such as the record pre-processing and NER. This is achieved by creating a `multiprocessing.Pool` which forks different processes that apply the entire pipeline to one record in the archive.

- **Multi-threading** for the parallelization of I/O bound tasks such as Elasticsearch and Trident queries. Unfortunately, we realized that the Trident Python client is not thread safe (nor we can instantiate multiple connections, see the Canvas thread at [this link](https://canvas.vu.nl/courses/55617/discussion_topics/456505)) so we had to limit the concurrent access to this resource with a mutex lock. We obtain threaded computation by creating a `multiprocessing.pool.ThreadPool` in each sub-process and performing tasks related to each entity in parallel.

Finally, we introduce caching behaviours to our pipeline at different stages.
We have built an ad-hoc decorator (given the functional programming style we adopted) and wrapped function bottlenecks with `@cached`.
In order to prevent errors and obtain a substantial speed-up, we isolate specific calls and carefully build our objects so that they are hashable (see our `src/interfaces.py`).
It should be stated that we programmed the caching behaviour under the strong assumption (based on the way the provided `score.py` script works) that a specific named entity would mantain the same meaning throughout the document.

## Results

The results presented are obtained by running the file with input the sample warc file provided (`sample.warc.gz`). The performance is measured by the F1-score, which is the weighted average of the precision and recall.

The F1-score we obtained is 4.5% (0.04529) with a recall of 6.7% (0.06770) and a precision of 3.4% (0.03402) on a Lenovo Thinkpad T14, Ryzen 7 4750 CPU, 32GB DDR4 RAM, 500GB M.1 NVMe SSD. On this platform, the computation took:

- **real**: 2m46.997s
- **user**: 4m38.971s
- **sys**: 1m13.028s

The computation is performed inside the provided Docker image `karmaresearch/wdps_assignment1` running inside the WSL 2 engine on a Windows 10 installation.

# References

<a id="ref_1">1.</a> Bird, S., Edward L. & Ewan K. (2009), Natural Language Processing with Python. O’Reilly Media Inc.

<a id="ref_2">2.</a> Honnibal, M., & Montani, I. (2017). spaCy 2: Natural language understanding with Bloom embeddings, convolutional neural networks and incremental parsing.
