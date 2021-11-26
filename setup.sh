
echo "Setting up Assignment 1 WDPS - Group 19"

echo "----"

echo "Upgrading 'pip'"

python3 -m pip install --upgrade pip

echo "Installing dependencies"

python3 -m pip install -r requirements.txt

python3 -m spacy download en_core_web_sm

echo "Starting ElasticSearch instance"
echo ""
echo ""
echo "[WARNING]: Make sure to have run 'sudo sysctl -w vm.max_map_count=262144' or the ElasticSearch client is going to crash!"
echo ""
echo ""

./assets/elasticsearch-7.9.2/bin/elasticsearch -d

echo ""
echo ""
echo "You can now run the program with the following command:"
echo "   python3 main.py <YOUR_WARC_GZ_ARCHIVE_PATH> > <OUTPUT_FILE_NAME>.tsv"


