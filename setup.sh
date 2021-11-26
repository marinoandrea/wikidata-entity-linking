start_elasticsearch=$1

echo "Setting up Assignment 1 WDPS - Group 19"
echo ""
echo ""
echo "Upgrading 'pip'"

python3 -m pip install --upgrade pip

echo ""
echo ""
echo "Installing dependencies"

python3 -m pip install \
    numpy \
    spacy \
    decorator \
    python-dateutil \
    elasticsearch \
    typing-extensions \
    beautifulsoup4 \
    scipy

python3 -m spacy download en_core_web_sm

echo ""
echo ""

if [ "$start_elasticsearch" = "--start-elasticsearch" ]; then
    echo "Starting ElasticSearch instance"
    echo ""
    echo ""
    echo "[WARNING]: You may need to run 'sudo sysctl -w vm.max_map_count=262144' on your host system or the ElasticSearch client is going to crash!"
    echo ""

    ./assets/elasticsearch-7.9.2/bin/elasticsearch -d

    echo "[WARNING]: Please wait for the Elastic Search client to start!"
    echo ""
    echo ""
fi

echo "You can now run the program with the following command:"
echo "   python3 main.py <YOUR_WARC_GZ_ARCHIVE_PATH> > <OUTPUT_FILE_NAME>.tsv"
echo ""
echo ""
