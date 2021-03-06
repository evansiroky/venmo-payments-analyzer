# Venmo Payment Analyzer

A python script to analyze venmo transactions to calculate the median degree of all transactions within a 60 second window.

## Installation

The script is intended to run with either Python 2 or 3, however it has only been tested with 2.7 and 3.4 at the moment.  No extra libraries are required to be installed except if running tests on the code (see [testing](#testing)).  The script runs on either windows or *nix platforms.  However, on windows, the script cannot be started using the `run.sh` script.

## Usage

To run the script invoke the script from the command line and provide two arguments.  The first argument is the input file to process and the second argument is the output file to write to.  The output file will automatically get created and any specified directory structure will also be attempted to be created.

Example:

```
python ./src/rolling_median.py ./venmo_input/venmo-trans.txt ./venmo_output/output.txt
```

### Input Data

The script requires the input file to exist.  The input file must be a plain text file having a valid json string on each line.  Each json string must contain the following keys and values:

| Key | Description |
| --- | --- |
| actor | A case-sensitive, non-empty string representing the ID of the actor in the transaction. |
| target | A case-sensitive, non-empty string representing the ID of the target in the transaction. |
| created_time | A string of the exact time in UTC of the transaction.  It must be in the format `YYYY-MM-DDTHH:MM:SSZ`.  Timezones other than "Z" are assumed invalid while parsing the data. |

Any lines that have either invalid JSON, missing keys or imporperly formatted date strings will be skipped in the analysis.

### Output Data

Data will be outputted to the file specified in the second command line argument.  The file will be a plain text file containing a line of output for each calculation of the median vertex degree that was performed.  The median vertex degree is the median amount of transactions in the most current 60 second window that each person has.  

For example if there are three transactions as follows:

```
Bob <> Jill
Jill <> Jeff
Jeff <> Gary
```

The number of transaction per person is as follows:

```
Bob: 1
Jill: 2
Jeff: 2
Gary: 1
```

Therefore the median number of transactions is 1.5.

## Testing

In addition to the tests provided by Insight, one additional test was written in python.  This file is located at `insight_testsuite\test_integration.py`.  The preferred test runner is [pytest](http://pytest.org/).  It is also recommended to use pytest in conjuction with [pytest-cov](https://pypi.python.org/pypi/pytest-cov) to generate coverage reports.

### Installation of testing packages

To install pytest and pytest-cov, use the following commands (this assumes that pip is installed with python):

```
pip install pytest
pip install pytest-cov
```

### Running of tests

To run the tests, browse to the root directory of the project and run the following command:

```
py.test
```

To run tests and generate a coverage report in html, run the the following command:

```
py.test --cov-report html --cov=src
```

The output to console will tell you where the coverage reports were stored.

### Implementation Notes

In order to optimize the data storage a edge list is stored in memory as the file is processed.  The edge list is implemented as a ordered linked list.  A linked list was chosen as the data structure in order to achieve fast inserts and updates of transactions.  Each time a new transaction is encountered the list of transactions is iterated through once.  This single iteration evicts old transactions, inserts the new transaction to maintain order by creation time, and also tallys up the count of transactions per person in a python Counter collection object.  If needed after iterating through the list of valid transactions, the median is calculated by simply sorting the transactions per person in the Counter collection and finding the median.

#### Complexity Analysis

The run-time complexity on each calculation of the median is O(E + V log V) where E is the number of edges of transactions and V is the number of people (vertices in the graph).  The V log V complexity is encountered in the sorting of transaction frequencies in the call to the most_common method of the Counter object.

The storage complexity is O(E + V) where E is the number of edges of transactions and V is the number of people (vertices in the graph).  The edges are stored in the edge list and storage is minimized for the vertices by simply tallying the count of transactions per person in the Counter object instead of maintaining a list of neighbors at the vertex level.
