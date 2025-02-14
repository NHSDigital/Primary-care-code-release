# Prerequisites for the PCD code release job

The PCD code release python scripts rely on the existence of the following resources to work:
* UK SCT release database – the content of the SNOMED release you will base your own content on. 
* Local SNOMED release database – your own SNOMED release database containing the content you maintain and publish.
* Cluster management database – contains information about your SNOMED reference sets which is not published in your SNOMED release.

## UK SNOMED CT release database

This is a SQL database which contains a version of the SNOMED content that your own release bases its content on, at a point in time. This is maintained in snapshot format and can be produced using the [SNOMED CT Implementation for Reporting guidance](https://www.google.com/url?sa=t&rct=j&q=&esrc=s&source=web&cd=&cad=rja&uact=8&ved=2ahUKEwjyza-jsaKBAxWTQkEAHbf8BvIQFnoECA0QAQ&url=https%3A%2F%2Fnhsengland.kahootz.com%2Fgf2.ti%2Ff%2F762498%2F134226021.1%2FDOCX%2F-%2FTerminology_Database_Tables_SNOMEDCTinPrimaryCare_UPDATED%25202022_0.4.docx&usg=AOvVaw19GuKrcIv_QOndycjelUGN&opi=89978449).

As a minimum your database will need the following tables:
* SCT_CONCEPT
* SCT_RELATIONSHIP
* SCT_DESCRIPTION
* SCT_REFSET
* SCT_TC

It should also contain your imported files which have been used to populate your tables. Preferred method is using the [UK monolith release files](https://isd.digital.nhs.uk/trud/users/guest/filters/0/categories/26/items/1799/releases).  

The database name is expected to include a date string in the format MmmYY which should relate to the UK release content you have based it on (e.g. the database containing the April 2024 UK content should contain Apr24).

Note: The variable MmmYY set in the config.toml user_inputs dictionary is incorporated into this database name so the c_sct_db value in the connections dictionary should be prefixMmmYYsuffix depending on your chosen naming convention.  

Assumed schema is dbo and the database is assumed to be on the same server as the Local SNOMED refset release database and the Cluster management database. 

## Local SNOMED refset release database

This is a SQL database containing your own SNOMED compliant release content stored in ‘full’ format. Guidance on structuring and maintaining a SNOMED release can be found in [SNOMED CT Release File Specifications](https://confluence.ihtsdotools.org/display/DOCRELFMT/SNOMED+CT+Release+File+Specifications).

As a minimum your database will need the following tables:
* Concept
* Description
* Language
* SimpleRefset
* Relationship
* OwlExpression
* Module Dependency
* Refset descriptor refset
* TRUD ID Generator

Unpublished data has a temporary effectiveTime of 99999999. 

Assumed schema is dbo and the database is assumed to be on the same server as the UK SNOMED CT release database and the Cluster management database. 

## Cluster management database

This is a SQL database containing your cluster records and any additional information relevant to your refsets which is not captured in your SNOMED release tables.

As a minimum your database will need the following tables:
* Clusters
* Cluster_Ruleset
* Cluster_Output
* Cluster_Population
* Rulesets
* Ruleset_Published
* Outputs
* Output_Ruleset
* Output_Population
* Populations
* Population_Ruleset
* Code_Decision_Log_Archive


The **Clusters** table should contain the following columns as a minimum:
* Cluster_ID 
* Cluster_Description
* Cluster_Category
* Refset_ID (refsetId in simple refset table)
* Cluster_ECL (expression constraint language string)
* Cluster_Maintained_By
* Cluster_Date_Inactive (date)

Optional additional columns:
* Cluster_Metadata
* Cluster Relationships
* Cluster_Authorisation

Cluster table records with a Cluster_Date_Inactive of null are the current active details. 

The **Cluster_Ruleset** table should contain the following columns as a minimum:
* Cluster_ID 
* Ruleset_ID
* Ruleset_Version
* Cluster_Ruleset_Date_Inactive (date)

Cluster_Ruleset table records with a Cluster_Ruleset_Date_Inactive of null are the current active details. 

The **Cluster_Output** table should contain the following columns as a minimum:
* Output_ID 
* Output_Version
* Cluster_ID
* Cluster_Output_Date_Inactive (date)

Cluster_Output table records with a Cluster_Output_Date_Inactive of null are the current active details. 

The **Cluster_Population** table should contain the following columns as a minimum:
* Population_ID 
* Population_Version
* Cluster_ID
* Cluster_Population_Date_Inactive (date)

Cluster_Population table records with a Cluster_Population_Date_Inactive of null are the current active details. 

The **Rulesets** table should contain the following columns as a minimum:
* Service_ID
* Ruleset_ID
* Ruleset_Version
* Ruleset_Date_Inactive (date)

Ruleset table records with a Ruleset_Date_Inactive of null are the current active details. 

The **Ruleset_Published** table should contain the following columns as a minimum:
* Service_ID
* Ruleset_ID
* Ruleset_Version
* Ruleset_Publication_Effective (date)
* Ruleset_Publication_Inactive (date)

Ruleset_Published table records with a Ruleset_Publication_Date_Inactive of null are the current active details. 

The **Outputs** table should contain the following columns as a minimum:
* Output_ID 
* Output_Version
* Output_Description
* Output_Type
* Previous_Output_Version

The **Output_Ruleset** table should contain the following columns as a minimum:
* Ruleset_ID
* Ruleset_Version
* Output_ID
* Output_Version
* Output_Ruleset_Date_Inactive (date)

Output_Ruleset table records with a Output_Ruleset_Date_Inactive of null are the current active details. 

The **Output_Population** table should contain the following columns as a minimum:
* Population_ID 
* Population_Version
* Output_ID
* Output_Version
* Output_Population_Date_Inactive (date)

Output_Population table records with a Output_Population_Date_Inactive of null are the current active details. 

The **Populations** table should contain the following columns as a minimum:
* Population_ID 
* Population_Version
* Population_Type
* Population_Description
* Previous_Population_Version

Populations table records with a Populations_Date_Inactive of null are the current active details. 

The **Population_Ruleset** table should contain the following columns as a minimum:
* Ruleset_ID
* Ruleset_Version
* Population_ID
* Population_Version
* Population_Ruleset_Date_Inactive (date)

Population_Ruleset table records with a Population_Ruleset_Date_Inactive of null are the current active details. 

The **Code_Decision_Log_Archive** table should contain the following columns as a minimum:
* Cluster
* Final_Decision (of: 'add', 'retain', 'do not add', 'exclude', 'remove', 'cancelled', 'awaiting', 'awaiting authorisation')
* Date_Actioned (YYYYMMDD varchar)

There are 3 stored procedures which will also need to be created:
* ADD_ruleset_publication = add a record to the Ruleset_published table
* UPDATE_ruleset_publication = add a record to the Ruleset_published table and sign off an old record
* RETIRE_ruleset_publication = and sign off an old record in the Ruleset_published table 

Assumed schema is dbo and the database is assumed to be on the same server as the UK SNOMED CT release database and the Local SNOMED refset release database.

