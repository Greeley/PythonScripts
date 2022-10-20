"""
Name: compare csv
Author: Dakota Carter
Description: compare two files together
"""

import sys
import re
import io
import os
import json
from datetime import datetime
import pandas as pd

import time
def timer_func(func):
    """
    decorator to time function
    :param func: function to time
    :return:
    """
    def wrap_func(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        print(f'Function {func.__name__!r} executed in {(t2-t1):.4f}s')
        return result
    return wrap_func

FILEPATHS = []

# CONSTANTS
DATABRICKS_NULL = '" "'
CLOUDERA_NULL = ' '
BAD_JSON = '{"ADDED":["PART'
JSON_REGEX = re.compile(r'^("(((?=\\)\\(["\\/bfnrt]|u[0-9a-fA-F]{4}))|[^"\\\0-\x1F\x7F]+)*")$')
BASIC_REPORT = {
    "JSON_ERRORS": 0,
    "LINE_COUNT_MATCH": False,
    "LINE_COUNT": [],
    "LINES_MATCH": 0,
    "LINES_NOT_MATCH": 0,
    "LINES_NOT_CHECKED": 0,
    "MISMATCH_LINES": [

    ]
}

MISMATCH_REPORT={
    "ITEM1": "",
    "ITEM2": ""
}


def capture_args(argv=None):
    """
    Capture the filepaths in this order: DATABRICKS CLOUDERA
    :param argv: the command used to run the script
    :return: sets global variables
    """
    global FILEPATHS
    in_file = "-f"
    in_list = "-l"
    for index, switch in enumerate(argv):
        if switch.startswith(in_file):
            FILEPATHS.append(argv[index+1])
        if switch.startswith(in_list):
            FILEPATHS.extend([fp for fp in open(argv[index+1], 'r').readlines()])
    if not any(FILEPATHS):
        print("Required: compare_csv databricks/file/path cloudera/file/path")
        exit(1)

def load_files(filepath1, filepath2):
    """
    Load files from S3 as list. not as pandas dataframe.
    :return: returns nothing
    TODO: Load Databricks File from Filepath
    TODO: Load Cloudera File from Filepath
    TODO: Load Both Files with `LATIN-1` or `ISO-8859-1` to support unicode characters!
    Note: latin-1 encodes ASCII the same as UTF-8 but has space for the first 256 Unicode characters
    """
    global DATABRICKS_RAW, CLOUDERA_RAW, DATABRICKS_FILEPATH, CLOUDERA_FILEPATH
    file1 = io.open(filepath1, 'r', buffering=1, encoding='latin-1')
    file2 = io.open(filepath2, 'r', buffering=1, encoding='latin-1')
    return file1, file2

def reset_files(file1, file2):
    """
    set cursor back to zero after looping through a file.
    :return:
    """
    file1.seek(0)
    file2.seek(0)


def _blocks(file, size: int=65536):
    """
    Generator for file to return data back in blocks.
    :param file: open file to "chunk" through
    :param size: change default blocksize from 65536
    :return:
    """
    while True:
        b = file.read(size)
        if not b: break
        yield b

def _count_lines(filepapth: str):
    """
    count the lines in very large files. using latin-1 for 127 characters support with unicode, ascii characters still
    are decoded and encoded via utf-8
    :param filepapth: the filepath to the file
    :return: (int) line_count
    """
    line_count = 0
    with open(filepapth, "r", encoding="latin-1", errors='ignore') as f:
        line_count +=sum(block.count("\n") for block in _blocks(f))
    return line_count

@timer_func
def check_line_count(filepath1, filepath2):
    """
    Count lines of each CSV which is equivalent to rows in spreadsheet.
    :return: exit 132 if fails
    """
    report_name = f"Comparision_Report_{os.path.basename(filepath1[:-4])}_currentDate"
    global BASIC_REPORT
    db_count = _count_lines(filepath1)
    cld_count = _count_lines(filepath2)
    if not db_count == cld_count:
        _exit_with_report(f"Counts DO NOT Match!\nDatabricks Rows: {db_count}\nCloudera Rows: {cld_count}", 103, report_name)
    BASIC_REPORT["LINE_COUNT"].extend([db_count, cld_count])
    BASIC_REPORT["LINE_COUNT_MATCH"] = True

def _normalize_databricks(db_item: str):
    """
    Normalize Databricks null values to cloudera null values.
    :return:
    """
    if DATABRICKS_NULL == db_item:
        return CLOUDERA_NULL
    return db_item

def _exit_with_report(reason: str, code: int, report_name: str):
    global BASIC_REPORT
    # print(f"TEST RESULTS:\n{BASIC_REPORT}\n\nEXITED!\nReason: {reason}\nerrorcode: {code}")
    current_date = datetime.now()

    file_name = report_name + "_" + current_date.strftime('%Y-%m-%dT%H-%M-%S')+".json"
    report_name_s3 = os.path.join("s3:bcbs-dev-db-data-layer/c720253/reports", current_date.strftime('%Y-%m-%d'))
    report_name_dbfs = os.path.join("/dfs/FileStore/CCR_EXTRACT_SRC_FILES/Comparision_reports/", current_date.strftime('%Y-%m-%d'))

    df = pd.DataFrame.from_dict(BASIC_REPORT)
    spark.write.json(os.path.join(report_name_s3, file_name), BASIC_REPORT)
    spark.write.json(os.path.join(report_name_dbfs, file_name), BASIC_REPORT)


    # spark.read.json(os.path.join(report_name, file_name))
    # dbutils.fs.mkdirs(report_name)
    # dbutils.fs.put(os.path.join(report_name, file_name),json.dumps(BASIC_REPORT, sort_keys=True, indent=4, separators=(',', ': '),
    #                   ensure_ascii=False))
    print(f"Writing results to: {os.path.join(report_name, file_name)}")


@timer_func
def check_files(file1, file2, filepath1, filepath2):
    """
    Use python quicksort to sort the file lists.
    if files are the same quicksort will sort both lists the same way.
    Don't need to care about contents of data, just that they match eachother.
    """
    report_name = f"Comparision_Report_{os.path.basename(filepath1[:-4])}_"

    global BASIC_REPORT
    print("Starting validations...")
    for index, (item1, item2) in enumerate(zip(sorted(file1), sorted(file2))):
        item1 = _normalize_databricks(item1)
        item2 = _normalize_databricks(item2)

        if index < 1:
            if BAD_JSON in item1.upper() or BAD_JSON in item2.upper():
                _exit_with_report(f"Found Bad JSON Value in text file:\nDatabricks: {item1}\nCloudera: {item2}",
                                  100, report_name)
        # elif JSON_REGEX.match(item1) or JSON_REGEX.match(item2):
            # print("Regular JSON Found: {}".format(item1 if JSON_REGEX.match(item1) else item2))
            #todo: if this is not supposed to fail comment the following and uncomment the above
            # _exit_with_report(f"Found JSON Value in text file:\nDatabricks: {item1}\nCloudera: {item2}",
            #                   100)

        if item1 == item2:
            BASIC_REPORT["LINES_MATCH"] += 1
        else:
            BASIC_REPORT["LINES_NOT_MATCH"] += 1
            temp = MISMATCH_REPORT.copy()
            temp["ITEM1"] = {"value": item1, "filepath": filepath1}
            temp["ITEM2"] = {"value": item2, "filepath": filepath2}
            BASIC_REPORT["MISMATCH_LINES"].append(temp)
            # _exit_with_report(f"DATABRICKS LINE: {item1}\nCLOUDERA LINE: {item2}\nItems do not Match",
            #                   101)
    else:
        print("Validations finished.")
        if BASIC_REPORT["LINE_COUNT_MATCH"]:
            BASIC_REPORT["LINES_NOT_CHECKED"] = BASIC_REPORT["LINE_COUNT"][0] - BASIC_REPORT["LINES_NOT_MATCH"]
        #todo: export report to file
        _exit_with_report("Success!", 0, report_name)



if "__main__" == __name__:
    # TODO: Which record mismatch
    # TODO: How many lines don't match
    capture_args(sys.argv)
    for filepath1, filepath2 in zip(FILEPATHS[::2], FILEPATHS[1::2]):
        file1, file2 = load_files(filepath1, filepath2)
        check_line_count(filepath1, filepath2)
        check_files(file1, file2, filepath1, filepath2)