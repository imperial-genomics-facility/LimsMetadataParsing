import sys,os,argparse
import libs.limsDataParsingFromAccessDb import load_data_from_access_db
import libs.processMetadata import generate_metadata_and_samplesheet

parser=argparse.ArgumentParser()
parser.add_argument('-a','--access_db_path', required=True, help='Path to Access LIMS db')
parser.add_argument('-q','--quote_file_path', required=True, help='Path to quote xls file')
parser.add_argument('-o','--output_path', required=True, help='Output dir path for metadta files')
parser.add_argument('-k','--known_projects_list', required=True, help='File containing list of known projects')
parser.add_argument('-j','--ucanaccess_jar_path', required=True, help='Pat to ucanaccess jar files')
args=parser.parse_args()

access_db_path = args.access_db_path
quote_file_path = args.quote_file_path
output_path = args.output_path
ucanaccess_jar_path = args.ucanaccess_jar_path
known_projects_list = args.known_projects_list

if __name__=='__main__':
  try:
    from pyspark.sql import SparkSession
    spark = \
      SparkSession.\
        builder.\
        appName('LimsMetadataParsing').\
        config("spark.sql.execution.arrow.pyspark.enabled", "true").\
        getOrCreate()
    project_data,sample_data,premadelibs_data,quotes_data = \
      load_data_from_access_db(\
        access_db_path=access_db_path,
        quotes_xls_path=quote_file_path,
        ucanaccess_jar_path=ucanaccess_jar_path)
    generate_metadata_and_samplesheet(\
      spark=spark,
      project_data=project_data,
      sample_data=sample_data,
      premadelibs_data=premadelibs_data,
      quotes_data=quotes_data,
      registered_projects_csv=known_projects_list,
      output_dir=output_path)
  except Exception as e:
    raise ValueError('Failed to parse metadata from access db, error: {0}'.format(e))