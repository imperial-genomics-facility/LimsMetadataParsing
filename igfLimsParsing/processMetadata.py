import os
import pandas as pd
from igfLimsParsing.dbTableSchema import get_table_schema
from pyspark.sql.functions import pandas_udf, PandasUDFType

def __get_metadata_df(spark):
  try: 
    all_new_project_metadata = \
    spark.sql("""
      select
      concat(project.QuoteID,'_', project.ProjectTag) as project_igf_id,
      sample.IGFID as sample_igf_id,
      sample.Species as species_text,
      sample.`Sample Name` as sample_submitter_id,
      project.Library_Preparation as library_preparation,
      sample.`Sample type` as sample_type,
      sample.`Sample description` as sample_description,
      library.Library_Name as library_name,
      library.Library_Type as library_type,
      quotes.Main_contact as name,
      quotes.Main_contact_email as email_id,
      quotes.Sequencing_Type as sequencing_type,
      quotes.Number_of_Lanes as expected_lanes
      from
      (
        select 
        table3.QuoteID
        from 
        (
          select
          project.QuoteID,
          project.ProjectTag,
          project.DateCreated,
          sample.IGFID,
          table2.project_qid,
          table2.project_igf_id
          from
          project
          left join
          sample
          on project.ID = sample.ProjectID
          left outer join
          (
            select
            registered_projects.project_igf_id,
            split(replace(registered_projects.project_igf_id,'-','_'),"_")[0] as project_qid
            from
            registered_projects
          )
          as table2
          on
          project.QuoteID==table2.project_qid
          where
          table2.project_igf_id is null
          and
          project.DateCreated > '2019'
          order by project.DateCreated desc
        )
        as table3
        group by table3.QuoteID
        having count(distinct table3.IGFID) > 0
      )
      as table4
      join project on project.QuoteID=table4.QuoteID
      join sample on project.ID = sample.ProjectID
      join library on project.ID=library.ProjectID and sample.`Sample Name`=library.`Library_Name`
      join quotes on project.QuoteID = quotes.QuoteID 
    """)
    return all_new_project_metadata
  except Exception as e:
    raise ValueError('Failed to parse tables for required metadata, error: {0}'.format(e))

def __get_samplesheet_df(spark):
  try:
    all_new_project_samplesheet = \
    spark.sql("""
      select
      sample.IGFID,
      library.Library_Name,
      library.Index_1_ID,
      library.Index1_Sequence,
      library.Index_2_ID,
      library.Index2_Sequence,
      concat(project.QuoteID,'_', project.ProjectTag) as project_igf_id,
      library.Pool_no
      from
      (
        select 
        table3.QuoteID
        from 
        (
          select
          project.QuoteID,
          project.ProjectTag,
          project.DateCreated,
          sample.IGFID,
          table2.project_qid,
          table2.project_igf_id
          from
          project
          left join
          sample
          on project.ID = sample.ProjectID
          left outer join
          (
            select
            registered_projects.project_igf_id,
            split(replace(registered_projects.project_igf_id,'-','_'),"_")[0] as project_qid
            from
            registered_projects
          )
          as table2
          on
          project.QuoteID==table2.project_qid
          where
          table2.project_igf_id is null
          and
          project.DateCreated > '2019'
          order by project.DateCreated desc
        )
        as table3
        group by table3.QuoteID
        having count(distinct table3.IGFID) > 0
      )
      as table4
      join project on project.QuoteID=table4.QuoteID
      join sample on project.ID = sample.ProjectID
      join library on project.ID=library.ProjectID and sample.`Sample Name`=library.`Library_Name`
      order by library.Pool_no
    """)
    return all_new_project_samplesheet
  except Exception as e:
    raise ValueError('Failed to parse tables for required samplesheet data, error: {0}'.format(e))

def generate_metadata_and_samplesheet(spark,project_data,sample_data,premadelibs_data,quotes_data,registered_projects_csv,output_dir):
  try:
    project_schema,sample_schema,library_schema,quotes_schema = \
      get_table_schema()
    OUT_DIR_BC = spark.sparkContext.broadcast(output_dir)
    project_df = spark.createDataFrame(project_data,schema=project_schema)
    sample_df = spark.createDataFrame(sample_data,schema=sample_schema)
    library_df = spark.createDataFrame(premadelibs_data,schema=library_schema)
    quotes_df = spark.createDataFrame(quotes_data,schema=quotes_schema)
    project_df.createOrReplaceTempView('project')
    sample_df.createOrReplaceTempView('sample')
    library_df.createOrReplaceTempView('library')
    quotes_df.createOrReplaceTempView('quotes')
    registered_projects = \
      spark.\
      read.\
      format('csv').\
      option('header','true').\
      option('path',registered_projects_csv).\
      option('sep','\t').\
      load()
    registered_projects.createOrReplaceTempView('registered_projects')
    all_new_project_metadata = __get_metadata_df(spark=spark)

    ## Pandas udf for metadata files
    @pandas_udf(all_new_project_metadata.schema, PandasUDFType.GROUPED_MAP)
    def __print_grp(pdf):
      try:
        if not isinstance(pdf,pd.DataFrame):
          raise TypeError('Expecting a Pandas df, got: {0}'.format(type(pdf)))
        out_dir = OUT_DIR_BC.value
        qid = pdf.drop_duplicates().get('project_igf_id').values[0]
        csv_output = os.path.join(out_dir,"{0}.csv".format(qid))
        pdf.drop_duplicates().to_csv(csv_output,index=False)
        return pdf
      except Exception as e:
        raise ValueError(e)

    metadata_records_count = \
      all_new_project_metadata.groupby('project_igf_id').apply(__print_grp).count()
    all_new_project_samplesheet = __get_samplesheet_df(spark=spark)

    ## Pandas udf for samplesheet files
    @pandas_udf(all_new_project_samplesheet.schema, PandasUDFType.GROUPED_MAP)
    def __print_samplesheet_grp(pdf):
      try:
        if not isinstance(pdf,pd.DataFrame):
          raise TypeError('Expecting a Pandas df, got: {0}'.format(type(pdf)))
        out_dir = OUT_DIR_BC.value
        qid = pdf.drop_duplicates().get('project_igf_id').values[0]
        csv_output = os.path.join(out_dir,"{0}_SampleSheet.csv".format(qid))
        pdf.drop_duplicates().to_csv(csv_output,index=False)
        return pdf
      except Exception as e:
        raise ValueError(e)

    samplesheet_records_count = \
      all_new_project_samplesheet.groupby('project_igf_id').apply(__print_samplesheet_grp).count()
  except Exception as e:
    raise ValueError('Failed to create new metadata and samplesheet files, error: {0}'.format(e))