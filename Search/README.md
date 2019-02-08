
# Description -
The notebook search utility is a python based command line utility which allows to search patterns or text across notebooks within an account. The utility generates two outputs 

1. CSV file - Provides notebook id, name , attached cluster details, paragraph no, paragraph title and line no(s) where the pattern(s) and or text matched.
2. Log file -  Prints the matching line and other details

# Usecase -
A common requirement where if a table name changes and its required to figure out the notebooks involved or doing some basic impact analysis.

# Features -
* Ability to search multiple text and\or patterns (regex - https://www.tutorialspoint.com/python/python_reg_expressions.htm)
* Ability to filter particular users
* Supports case sensitive searches


# Prerequisites -
Pandas Library (pip install pandas)

# How to run it -
Place the "notebookSearch.py" in a directory. It works with python 2.7 and python 3.x. To see all parameter options - "python notebookSearch.py -h"

Example - The following is looking for matching text patterns that begin with "emp_" and ends with "_stg".

python notebookSearch.py -e "api" -t $AUTH_TOKEN -p "emp_(.*)_stg" -u "<user_email_id>" 

