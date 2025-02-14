# PCD-SNOMED-Release

## Description
This repository contains the code and supporting templates associated with a Primary Care Domain SNOMED Release. 

__Purpose:__ Clusters or reference sets (refsets) are terms used to describe a categorised assortment of clinical codes. These are referred to within the Primary Care Domain team, specifically in GPDESA business rules. These are maintained throughout the year and change in response to customer requests, clinician review and changes within the UK and International SNOMED code releases. GPDESA maintains the content of clusters/refsets and publishes these as a SNOMED release pack on TRUD. These release packs are published in a very specific format and have to pass through a large series of checks before they can be published.

__Operational environment:__ Clinical data.

__Intended users:__ Primary care analysts or those who may manage large and dynamically changing terminology datasets.

__Guidance:__
For internal use: For guidance on how to create, check and publish a Primary Care Domain (PCD) SNOMED release pack, please check our Knowledge Hub Confluence page.

For external use: Please see in the "docs" folder -> "PCD_release_prerequisites.md" for additional information on database structure and setup.

The following information relates to how to setup and run the scripts correctly.

## Setup
For internal use: We recommend running this in RDS and using Visual Studio (VS) Code.

### Downloading / re-installing the repo project
To run a one-off time of this project:

* Download the main branch of the repo (button to the left of the "Clone" button) and save to an appropriate location that you can access (ideally named under the code release being run, e.g. "Oct23".). If this downloads as a zip file, you will need to unzip the contents.
* ++ Open up VS Code, and select 'Open Folder'. Open your 'pcd-snomed-release(-main)' folder. Ctrl+' will open the terminal at the bottom of VS Code. You will need this for virtual environments and running the code.

If you have previously Cloned the repo project:
* As you have a local copy of the project cloned, it is important to make sure that this copy is up to date and current as the remote (github internet version) repo. 
* Firstly, check that you are correctly on the main branch. `git checkout main ` If you are on a development branch, then make sure to save, add and commit any changes. 
* Remove any amendments made on any last runs of the project:
`git reset --SOFT ` for resetting commit histories
`git reset --MIXED ` for resetting the above & any staging indexes
`git reset --HARD ` for resetting the above & all changes in the working directory
* update your local copy to be the latest from the remote repo: `git pull `
* ++ Re-run this action.

### Virtual Environment
In the terminal, type the following to create a virtual environment called pcd-snomed-release. This will install the correct versions of packages to run this code:

- create the venv enviroment (only run once at initial set up) - *(~2-10 mins)*
`py -m 3.10 venv .venv/pcd-snomed-release` 
OR the below command if the first does not work:
`C:\Python310\python.exe -m venv .venv/pcd-snomed-release`

- activate the venv environment (run each time you need to use the code) - *(~1-2 mins)*
`.venv/pcd-snomed-release/Scripts/activate.ps1`

The code above may throw up a security warning. Type [R] Run once. You should then get '(pcd-snomed-release)' at the beginning of the line in the terminal to show the virtual environment has been activated. Continue by running the following:

- upgrade pip to its latest version (only need to run once at initial set up) - *(~2-5 mins)*
`py -m pip install --upgrade pip` 

- create the venv environment (only need to run once at initial set up) - *(~5-20 mins)*
`pip install -r setup/requirements.txt`

This virtual environment setup process will take a couple of minutes to complete.

### User inputs/parameters - *(~5-20 mins)*
You will need to open up the config.toml file in the main repo and change the inputs to match your code release. PLEASE READ OVER THE ENTIRE FILE, checking every input before running, even if you do not think they need altering. For internal use, typically it will only be [Setup], [Dates] and [Filepaths] that require changes. Make sure to __save__ this file after you have finished making the changes, otherwise python will run it through with the latest saved version.

Please note, if you are adding additional information to the UK NHS Primary Care data extraction reference sets word document, that the config file does not have a character limit. The text should be inserted between triple quotation marks ("""insert text here"""). The text can be over multiple lines if need be.

The scripts contain a few user inputs. Please keep an eye on the terminal for any questions and answer in the format requested or the script will error out.

If you are running the tests (back tests or unit tests), you will need to change the test_config.toml file in the tests folder of the repo.

## Running the code
If you wish to change your python interpreter:
* Ctrl+Shift+P
* Python: Select Interpreter
* Select your virtual environment. It should look something like 'Python 3.10.5 ('pcd-snomed-release':venv)
* If the virtual environment has not come up as an option, click 'Enter interpreter path...' and paste in the location of your pcd-snomed-release virtual environment, e.g. "C:\Users\Documents\pcd-snomed-release-main\.venv\pcd-snomed-release2"
    * Please make sure you have navigated to the virtual environment folder name inside the .venv file
    * Re-do Ctrl+Shift+P, Python: Select Interpreter, and check that your virtual environment is now selected

Alternatively, if you want to just run the script from the terminal (paste below code and then enter on keyboard), for running the three main scripts, type:

`python main_script_1.py` *(~3 mins)*

`python main_script_2.py` *(~20-50 mins)*

`python main_script_3.py`   *(~8 mins)*

`python ECC_run_only_script.py` *(~15-20 mins)*

To run the unit tests:

`pytest tests/unittests`

## Project Folder and key files structure
```text

+---.vscode
+---.venv
|   +---pcd-snomed-release
+---docs
|       CODEOWNERS
|       CODE_OF_CONDUCT.md
|       CONTIBUTING.md
|       LICENSE
|       PCD_release_prerequisites.md
|       README.md
|       SECURITY.md
|
+---outputs
|   +---002_Tables_for_Power_BI_report
|   +---004_txt_files
|   +---999_05.12.24
|   |   +---Changed_ECC_Services
|   |   +---Changed_Expanded_cluster_lists_Ruleset-level
|   |   |   +---Live_Rulesets
|   |   |   \---Upcoming_Rulesets
|   |   +---Changed_Expanded_cluster_lists_Service-level
|   |   |   +---Live_Services
|   |   |   +---Upcoming_Services
|   |   +---Static_Expanded_cluster_lists_Ruleset-level
|   |   \---Static_Expanded_cluster_lists_Service-level
|   |
|   +---PreWork
|   +---prework_fldr
|   \---SnomedCT_UKPrimaryCareRF2_PRODUCTION_20241205T000000Z
|       +---Delta
|       |   +---Refset
|       |   |   +---Content
|       |   |   +---Language
|       |   |   +---Map
|       |   |   \---Metadata
|       |   \---Terminology
|       +---Documentation
|       +---Full
|       |   +---Refset
|       |   |   +---Content
|       |   |   +---Language
|       |   |   +---Map
|       |   |   \---Metadata
|       |   \---Terminology
|       +---Snapshot
|       |   +---Refset
|       |   |   +---Content
|       |   |   +---Language
|       |   |   +---Map
|       |   |   \---Metadata
|       |   \---Terminology
|       \---SupportingProducts
|
+---setup
|   |   config.toml
|   |   requirements.txt
|
+---sql
|
+---src
|   +---release_stages
|   \---utils
|
+---templates_and_inputs
|   \---testing
\---tests
|    |   test_config.toml
|    \---unittests
|        +---test_release_stages
|        +---test_utils
|        \---unittest_folder_to_move
|        
|       ECC_run_only_script.py
|       main_script_1.py   
|       main_script_2.py   
|       main_script_3.py   
        
```
## Expect Outputs:
**Folders: ()**
* ukpc.sct2_X.0.0_YYYYMMDD folder
* Publication folder: 000_dd.mm.yy (ReleaseNumber_PublicationDate)
* ECC specific subfolders

**Files: (~10)**
* GDPPR_Reference_data_creation_QA_errors_to_investigate.csv >>> only if there are errors in the SCT sql table to investigate. Code will also error out at this point.
* GDPPR_Cluster_Refset_1000230_YYYYMMDD.csv
* GPData_Cluster_Refset_1000230_YYYYMMDD.csv

* Firearms_trigger_refset_membership_YYYYMMDD.xlsx
* C19_SNOMED_changes_YYYYMMDD.xlsx

* YYYYMMDD_GPdata_and_GDPPR_files.zip 
* zipped ukpc.sct2_X.0.0_YYYYMMDD

* TRUD Pack Request Document (TRUD Pack Request_VXX.0.0.docx)
* doc_UKSNOMEDCTPrimaryCareDataExtractionsOverview_Current-en_GB_YYYYMMDD.docx

* Logger.log file for each Script ran

**Emails: (5)**
* 'GDPPR and GPData Reference data.eml' (with attachments) 
* 'Zipped Refset Release vXX.0.0.eml' (with attachments) 
* 'GDPPR SPL cluster content changes YYYYMMDD.eml' (with attachments) 
* 'PCD SNOMED Release Suppliers Sharepoint vXX.0.0.eml' 
* 'DGAT_RCL_changes_post_code_release.eml' (with attachments) (RCL - Ruleset code list)

## License
This codebase is released under the MIT License. This covers both the codebase and any sample code in the documentation.

Any HTML or Markdown documentation is [© Crown copyright](https://www.nationalarchives.gov.uk/information-management/re-using-public-sector-information/uk-government-licensing-framework/crown-copyright/) and available under the terms of the [Open Government 3.0 licence](https://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/).

## Contributing
Contributions to this project are welcome from anyone, providing that they conform to the [guidelines for contribution](https://github.com/NHSDigital/personal-demographics-service-api/blob/master/CONTRIBUTING.md) and the [community code of conduct](https://github.com/NHSDigital/personal-demographics-service-api/blob/master/CODE_OF_CONDUCT.md).


## Publication Links
* Primary Care Domain reference set portal: (https://digital.nhs.uk/data-and-information/data-collections-and-data-sets/data-collections/quality-and-outcomes-framework-qof/quality-and-outcome-framework-qof-business-rules/primary-care-domain-reference-set-portal)
* Primary Care Domain reference set portal (PCD refsets interactive portal): (https://app.powerbi.com/view?r=eyJrIjoiZTY0ODY0YzEtMjhhYy00ZTViLWJjZDQtMWZjOWVkMTZlOGExIiwidCI6IjUwZjYwNzFmLWJiZmUtNDAxYS04ODAzLTY3Mzc0OGU2MjllMiIsImMiOjh9)

## Authors and Primary Contact 
* Lead Authors: Laura Corbett, Jo Parker, Heather Taylor
* Contact: General Practice Specification and Extract Service - [GPDESA](england.gpses@nhs.net)