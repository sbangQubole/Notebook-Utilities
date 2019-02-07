
# What is the purpose of this utility?

The “Export Notebook Code” notebook exports code from other quoble notebook. The notebook needs to imported and run on a live cluster. The notebook takes in different arguments. Arguments are documented below.  Utility skips exporting context for following interpreters [ %angular, %dep, %md, %sh, %knitr, %spark.knitr, %spark.sql and %sql ]. For spark 2.0 and above.



# What major tasks does the utility performs?

It performs the following tasks -
* Allows you to export code (Python/R/Scala/All) to files(.py/.R/.scala) or console.

Optionally –
* It can save the file on cloud storage
* It can add spark session to exported code by using the information from the interpreters
* It can generate spark submit to run the exported code via Qubole Analyze for Python & R


# Any Limitations?

* If code uses zeppelin context (z.), this utility does not take any action on it.
* It does not take into account dependenices set via %dep interpreter.


# What use-cases does it solve?

* Export ETL development done via Notebooks to run via Spark Submit command.
* Share reusable code via a notebook (Python & R). Export the code to ".py" or ".r" file and then use sc.addPyFile("cloud_storage_path_of_exported_code_file") or sc.addFile("cloud_storage_path_of_exported_code_file") respectively to import code in other notebooks.
* To perform code comparisons.


# Notebook Arguments –

Notebook takes in 6 arguments:
* **notebookName** – Name of the notebook whose code's need to be exported. It's mandatory.
* **outputLang** - The language (python,scala,r) of the notebook.  You can also specify "ALL" to export code for comparison.
* **cloudStorageFile** – Filename along with its cloud path (s3,blob,adls,etc) to write the exported code. It's not mandatory. When value set to None or empty, exported code will be displayed on the console.
* **addSparkSession** – Adds spark session code based on language to exported code. Default value is "Y".
* **generateSparkSubmit** – Generates spark submit code to run via Qubole Analyze Shell command. Default value is "Y". It's disabled when outputLang is "All" or "Scala".
* **moveSparkConfToSparkSession** – Move sparkConf code to spark submit. Default value is "N" which means spark configs are added to spark session within the exported code.


# Demo Run -


### Python - 

![](PySparkExportCode.gif)



### Scala - 

![](ScalaExportCode.gif)



### R - 

![](RExportCode.gif)
