import os, jaydebeapi
import pandas as pd

def load_data_from_access_db(
      access_db_path, quotes_xls_path, ucanaccess_jar_path,
      required_ucanaccess_jars=(
        'ucanaccess-4.0.4.jar',
        'lib/commons-lang-2.6.jar',
        'lib/commons-logging-1.1.3.jar',
        'lib/hsqldb.jar',
        'lib/jackcess-2.1.11.jar')):
  try:
    project_data,sample_data,premadelibs_data,quotes_data = \
      __read_data_from_access_and_xls_file(
        access_db_path=access_db_path,
        quotes_xls_path=quotes_xls_path,
        ucanaccess_jar_path=ucanaccess_jar_path,
        required_ucanaccess_jars=required_ucanaccess_jars)
    return project_data,sample_data,premadelibs_data,quotes_data
  except Exception as e:
    raise ValueError('Failed to parse access db, eooro: {0}'.format(e))

def __read_data_from_access_and_xls_file(
      access_db_path, quotes_xls_path, ucanaccess_jar_path,
      required_ucanaccess_jars):
  try:
    ucanaccess_jars = list()
    for jar in required_ucanaccess_jars:
      ucanaccess_jars.append( os.path.join(ucanaccess_jar_path,jar))
    classpath = ":".join(ucanaccess_jars)
    project_data = list()
    sample_data = list()
    premadelibs_data = list()
    quotes_data = pd.DataFrame()
    try:
        cnxn = \
          jaydebeapi.connect(
            "net.ucanaccess.jdbc.UcanaccessDriver",
            f"jdbc:ucanaccess://{access_db_path};newDatabaseVersion=V2010",
            ["", ""],
            classpath)
        crsr = cnxn.cursor()
        premadelibs_data_columns = [\
          "ID",
          "ProjectID",
          "Container_ID",
          "Well_Position",
          "Library_Name",
          "Library_Type",
          "Library_Prep_Kit_Manufacturer",
          "Library_Prep_Kit_Name",
          "Library_Prep_Kit_Catalogue_No",
          "Index_1_ID",
          "Index1_Sequence",
          "Index1_Length",
          "Index_2_ID",
          "Index2_Sequence",
          "Index_2_Length",
          "Library_Volume_ul",
          "Library_Average_Fragment_Size_bp",
          "Library_Concentration_ng/ul",
          "nM_library",
          "Pool_No",
          "Spike-in_PhiX_PCT",
          "Read_1_Primer_name",
          "Read_2_Primer_name",
          "Index_1_Primer_name",
          "Index_2_Primer_name",
          "Volume_ul_Pool",
          "Pool_Average_Fragment_Size_bp",
          "Pool_Concentration_ng/ul",
          "Pool_nM",
          "Premade",
          "Status",
          "Workflow",
          "ContainerType",
          "Species",
          "LIbraryID"]
        crsr.execute("""
        select 
        `ID`,
        `ProjectID`,
        `Container_ID`,
        `Well_Position`,
        `Library_Name`,
        `Library_Type`,
        `Library_Prep_Kit_(Manufacturer)`,
        `Library_Prep_Kit_(Name)`,
        `Library_Prep_Kit_(Catalogue_No)`,
        `Index_1_ID`,
        `Index1_(Sequence)`,
        `Index1_(Length)`,
        `Index_2_(ID)`,
        `Index2_(Sequence)`,
        `Index_2_(Length)`,
        `Library_Volume_(µl)`,
        `Library_Average_Fragment_Size_(bp)`,
        `Library_Concentration_(ng/µl)`,
        `nM_(library)`,
        `Pool_No`,
        `Spike-in_PhiX_(%)`,
        `Read_1_Primer_name`,
        `Read_2_Primer_name`,
        `Index_1_Primer_name`,
        `Index_2_Primer_name`,
        `Volume_(µl)_(Pool)`,
        `Pool_Average_Fragment_Size_(bp)`,
        `Pool_Concentration_(ng/µl)`,
        `Pool_nM`,
        `Premade`,
        `Status`,
        `Workflow`,
        `ContainerType`,
        `Species`,
        `LIbraryID`
        from premadelibs2""")
        for row in crsr.fetchall():
          premadelibs_data.append(row)
        premadelibs_data = pd.DataFrame(premadelibs_data,columns=premadelibs_data_columns)
        sample_columns=[
          'ID',
          'ProjectID',
          'IGFID',
          'Container ID',
          'Well position',
          'Sample number',
          'Sample Name',
          'Sample type',
          'Species',
          'Sample description',
          'Volume (ul)',
          'Diluent',
          'Concentration (ng/ul)',
          'Quantification method',
          'Bionalyzer trace provided',
          'RIN value',
          'DNAse treated?',
          'Pool number',
          'Workflow',
          'Status',
          'Concentration (ng/ul) -internal value',
          'Trace file',
          'ContainerType',
          'SampleQCPass']
        crsr.execute("""
          select
          `ID`,
          `ProjectID`,
          `IGFID`,
          `Container ID`,
          `Well position`,
          `Sample number`,
          `Sample Name`,
          `Sample type`,
          `Species`,
          `Sample description`,
          `Volume (ul)`,
          `Diluent`,
          `Concentration (ng/ul)`,
          `Quantification method`,
          `Bionalyzer trace provided`,
          `RIN value`,
          `DNAse treated?`,
          `Pool number`,
          `Workflow`,
          `Status`,
          `Concentration (ng/ul) -internal value`,
          `Trace file`,
          `ContainerType`,
          `SampleQCPass` from samples""")
        for row in crsr.fetchall():
          sample_data.append(row)
        sample_data = pd.DataFrame(sample_data,columns=sample_columns)
        project_columns = [
          'ID',
          'QuoteID',
          'ProjectID',
          'DateCreated',
          'SignedQuoteReceived',
          'DateSamplesReceived',
          'SampleReceived',
          'LegacyProjectID',
          'Initial_QC',
          'Number_of_items_for_Initial_QC',
          'IQC_Unit_price',
          'Library_Preparation',
          'Number_of_items_for_Library_Prep',
          'LP_Unit_price',
          'Library_QC',
          'Number_of_items_for_Library_QC',
          'LQC_Unit_price',
          'Sequencing_Type',
          'Number_of_Lanes',
          'Seq_Unit_price',
          'Data_analysis',
          'Number_of_items_for_Data_Analysis',
          'DA_Unit_price',
          'Total_Quoted',
          'Price_Band',
          'Cost_of_Project',
          'estimated_surplus',
          'ProjectSummary',
          'VAT',
          'Grant Code',
          'Issued',
          'ProjectTag',
          'Comments',
          'Status',
          'Customer Note',
          'Library QC_summary',
          'Sequencing_summary',
          'Data Analysis_summary',
          'Misc_summary',
          'Misc1',
          'Misc1_description',
          'Misc1_qty',
          'Misc1_unitprice',
          'Instrument Access',
          'InsAcc_description',
          'InsAcc_qty',
          'InsAcc_unitprice',
          'Main_contact',
          'Main_contact_email']
        crsr.execute("""
          select
          `ID`,
          `QuoteID`,
          `ProjectID`,
          `DateCreated`,
          `SignedQuoteReceived`,
          `DateSamplesReceived`,
          `SampleReceived`,
          `LegacyProjectID`,
          `Initial_QC`,
          `Number_of_items_for_Initial_QC`,
          `IQC_Unit_price`,
          `Library_Preparation`,
          `Number_of_items_for_Library_Prep`,
          `LP_Unit_price`,
          `Library_QC`,
          `Number_of_items_for_Library_QC`,
          `LQC_Unit_price`,
          `Sequencing_Type`,
          `Number_of_Lanes`,
          `Seq_Unit_price`,
          `Data_analysis`,
          `Number_of_items_for_Data_Analysis`,
          `DA_Unit_price`,
          `Total_Quoted`,
          `Price_Band`,
          `Cost_of_Project`,
          `estimated_surplus`,
          `ProjectSummary`,
          `VAT`,
          `Grant Code`,
          `Issued`,
          `ProjectTag`,
          `Comments`,
          `Status`,
          `Customer Note`,
          `Library QC_summary`,
          `Sequencing_summary`,
          `Data Analysis_summary`,
          `Misc_summary`,
          `Misc1`,
          `Misc1_description`,
          `Misc1_qty`,
          `Misc1_unitprice`,
          `Instrument Access`,
          `InsAcc_description`,
          `InsAcc_qty`,
          `InsAcc_unitprice`,
          `Main_contact`,
          `Main_contact_email`
          from projects""")
        for row in crsr.fetchall():
          project_data.append(row)
        project_data = pd.DataFrame(project_data,columns=project_columns)
        project_data['DateCreated'] = \
          pd.to_datetime(project_data['DateCreated'].fillna(''),format="%Y-%m-%d %H:%M:%S")

        crsr.close()
        cnxn.close()
    except Exception as e:
      crsr.close()
      cnxn.close()
      raise ValueError('Failed to fetch data from access db: {0}'.fromat(e))
    quotes_data = pd.read_excel(quotes_xls_path)
    return project_data,sample_data,premadelibs_data,quotes_data
  except Exception as e:
    raise ValueError('Failed to fetch metadata from access and xls files: {0}'.format(e))