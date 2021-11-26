# WDPS 2021 - Assignment 1

This is the project repository for the group assignment 1 in the course Web Data Processing Systems (WDPS) at the Vrije Universiteit, Amsterdam for the academic year 2021-2022 by group 19.
The task of the assignment is to extract named entities from web pages and link them to potential entity candidates in the WikiData knowledge base.

The following is an overview of the functionality provided by our program and of the different steps in our data pipeline:

- Page metadata parsing and raw text-from-HTML extraction
- Natural Language Processing (NLP) pre-processing (tokenization, pos tagging etc.)
- Named entity recognition (NER)
- Candidate ranking and linking


## Setup

We assume that the reader will be running this program inside the `karmaresearch/wdps_assignment1` docker container and to also have the `data` and `assets` folder with the wikidata and elasticsearch data available.

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

WARNING: in the setup, we included the command to start the Elastic Search server. However, we encountered an error in the Elastic Search startup when the following command was not run in the host environment before running the setup script:

    sudo sysctl -w vm.max_map_count=262144

WARNING: this may raise some errors during the installation of additional python dependencies. We noticed that additional system-level peer dependencies may have to be installed (although we were able to run the program without). If necessary, you should be able to install them using the following command:

    sudo apt install libcairo2-dev

After setup is complete you can run the program in the following way:

    python3 main.py <INPUT_WARC_GZ_ARCHIVE_PATH> > <OUTPUT_TSV_FILE_PATH>

The `INPUT_WARC_GZ_ARCHIVE_PATH` must be an existing `.warc.gz` file path in the docker container.

## Data Preprocessing 

Once the WARC records are extracted from the archive, we parse each record's metadata with regular expressions (with the most important piece of information being the “id” to differentiate between extracted records). 

Then, we skip the HTTP headers and extract the HTML from the page. The HTML tags are processed using the `beautifulsoup` python package in order to only output the raw text.

## Entity Recognition 

Once the raw text is extracted from the HTML page, we proceed to extract the relevant named entities. To perform this task, two different tools have been considered and applied: the `nltk` python package (“natural language toolkit” <a href="#ref_1">[1]</a>) and a NER tool integrated in SpaCy <a href="#ref_2">[2]</a>, another python library.
 
In the former case, only tokenization, stopword removal, PoS tagging and parsing were performed while SpaCy provides a trained English language model that performs the same tasks and while also providing named entity recognition (NER).

SpaCy additionally labels each entity to provide some contextualization.
We relied on this preliminary labels to restrict our search of candidates in the next phases. 

## Candidate Generation and Entity Linking 

In order to generate candidates we perform a search based on string similarity through Elasticsearch in order to obtain related documents in the Wikidata for the given entity. 
Once the response from Elasticsearch is received, a list of candidate objects is created comprising the id, the similarity score (from ElasticSearch) and the available text information.

Several methods were considered for linking the candidates:

1. On a first experimentation, only the Elasticsearch score was used to rank the candidates but it proved to be very inaccurate. 

2. The next approach we considered, which is now part of our implementation, is to use the SpaCy entity label to score the candidates in the list based on context dependent features available by querying the knowledge base (via Trident). For this strategy, we built queries based on pregress knowledge that could use the relationships in the knowledge base graph to produce some 'similarity' score for a given candidate given a label.
However, we observed that the labels we obtain from SpaCy are very general and encompass a wide range of possible entities which translates to a few percentage points improvement from the first method. 
In some cases we were able to identify a superclass which would accurately map back to a SpaCy entity label. However, in most cases we had to construct ad-hoc queries to produce significant scores for the candidates’ “compliance” to such labels which also introduced human bias in the equation.
More specifically, manual pattern recognition is performed on various examples obtained by searching through the knowledge base of Wikidata, where mostly the properties P31 and P279; “instance-of” and “subclass-of” respectively are leveraged to favour attributes that are present in specific entity types in order to maximize correct entity linking. Thus, for each entity class, the patterns identified add further points to each type and the type with the highest score is preferred as it is the most likely type according to the model developed.

3. Finally, we experimented with **cosine similarity** between the named entity word vectors (also produced through SpaCy) and its candidates. However, this implementation did not perform significantly better and we decided to leave it out of the pipeline (the source code is still present and available to read) as it was more computationally intensive.

## Scalability and Efficiency 

Parallelization was essential to make use of all available resources efficiently. 
We took a twosided approach and decided to implement:

- **Multi-processing** for the parallelization of CPU bound tasks such as the record preprocessing and NER. We obtained this by creating a `multiprocessing.Pool` which forks different processes that apply the entire pipeline to one record in the archive.

- **Multi-threading** for the parallelization of I/O bound tasks such as Elastic Search and Trident queries. Unfortunately, we realized that the Trident Python client is not thread safe (nor we can instantiate multiple connections, see the Canvas thread at [this link](https://canvas.vu.nl/courses/55617/discussion_topics/456505)) so we had to limit the concurrent access to this resource with a mutex lock. We obtain threaded computation by creating a `multiprocessing.pool.ThreadPool` in each sub-process and performing tasks related to each entity in parallel.

Finally, we decided to introduce caching behaviours to our pipeline at different stages. 
We built an ad-hoc decorator (given the functional programming style we adopted) and wrapped function bottlenecks with `@cached`. 
In order to prevent errors and obtain a substantial speedup we had to isolate specific calls and carefully build our objects so that they were hashable (see our `src/interfaces.py`). 
It should be stated that we programmed the caching behaviour under the strong assumption (based on the way the provided `score.py` script works) that a specific named entity would mantain the same meaning throughout the document.

## Results

The results presented were obtained by running the file with input the sample warc file provided (`sample.warc.gz`). The performance was measured by the F1-score, which is the weighted average of the precision and recall. 

The f1 score we obtained is 3.6% (0.036) with a recall of 5.3% (0.053) and a precision of 2.7% (0.027) on a Lenovo Thinkpad T14, Ryzen 7 4750 CPU, 32GB DDR4 RAM, 500GB M.1 NVMe SSD. On this platform, the computation took:

- **real**: 2m38.067s
- **user**: 5m20.167s
- **sys**: 1m13.334s

The computation was performed inside the provided Docker image `karmaresearch/wdps_assignment1` running inside the WSL 2 engine on a Windows 10 installation.

# References

<a id="ref_1">1.</a> Bird, S., Edward L. & Ewan K. (2009), Natural Language Processing with Python. O’Reilly Media Inc.

<a id="ref_2">2.</a> Honnibal, M., & Montani, I. (2017). spaCy 2: Natural language understanding with Bloom embeddings, convolutional neural networks and incremental parsing.