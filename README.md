# LimsMetadataParsing
A pyspark based codebase for fetching and formatting metadata from a LIMS db for IGF

## Set up environment

* Step 1: Get Miniconda
<pre><code>wget https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
</pre></code>

* Step 2: Clone git repo
<pre><code>git clone https://github.com/imperial-genomics-facility/LimsMetadataParsing.git
</code></pre>
* Step 3: Install conda env from the environment.yml file
<pre><code>conda env create -n ENV_NAME --file environment.yml
</code></pre>
* Step 4: Create egg file for LimsMetadataParsing repo
<pre><code>python setup.py bdist_egg
</code></pre>

## Get UCanAccess

Download UCanAccess from the following link and unzip the contents
  - [http://ucanaccess.sourceforge.net/site.html](http://ucanaccess.sourceforge.net/site.html)

## Usage
<pre><code>parseAccessDbForMetadata.py [-h] -a ACCESS_DB_PATH -q QUOTE_FILE_PATH
                                 -o OUTPUT_PATH -k KNOWN_PROJECTS_LIST -j
                                   UCANACCESS_JAR_PATH

optional arguments:
  -h, --help                show this help message and exit
  -a ACCESS_DB_PATH, --access_db_path ACCESS_DB_PATH
                            Path to Access LIMS db
  -q QUOTE_FILE_PATH, --quote_file_path QUOTE_FILE_PATH
                            Path to quote xls file
  -o OUTPUT_PATH, --output_path OUTPUT_PATH
                            Output dir path for metadta files
  -k KNOWN_PROJECTS_LIST, --known_projects_list KNOWN_PROJECTS_LIST
                            File containing list of known projects
  -j UCANACCESS_JAR_PATH, --ucanaccess_jar_path UCANACCESS_JAR_PATH
                            Path to ucanaccess jar files
</pre></code>
## Run spark code

<pre><code>spark-submit \
--master local[NUMBER_OF_CPUS] \
--py-files /path/igfLimsParsing-0.0.1-py3.6.egg \
/path/LimsMetadataParsing/scripts/parseAccessDbForMetadata.py \
-a /path/Database.accdb \
-q /path/Quotes.xlsx \
-o /path/csv_dir \
-k /path/project_list.csv \
-j /path/UCanAccess-4.0.4-bin
</code></pre>

