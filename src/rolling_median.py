# -*- coding: utf-8 -*-
"""Rolling Median Calculation

This script ingests a text file containing venmo transactions in json format.

"""

from collections import Counter
from datetime import datetime
import json
import os
import sys


class Transaction:
    """
    Transactions are stored as nodes to be saved in a linked list.
    This allows for fast insertions and deletions.

    Attributes:
        next (Transaction): Next transaction with time >= this transaction's time.
        time (datetime): Time the transaction occurred.
        target (str): Unique case-sensitive identifying name of payee
        actor (str): Unique case-sensitive identifying name of payer
    """

    def __init__(self, json_string):
        """
        Constructor for Transaction.

        Parses a json string as the data.

        Args:
            json_string: a string containing the following fields:
                - created_time: A datestring with the format YYYY-MM-DDTHH:MM:SSZ.
                - target: Unique case-sensitive identifying name of payee
                - actor: Unique case-sensitive identifying name of payer
        """

        self.next = None

        data = json.loads(json_string)
        self.time = datetime.strptime(data['created_time'], '%Y-%m-%dT%H:%M:%SZ')
        self.target = data['target']
        self.actor = data['actor']


def insert(insert_transaction, prev_transaction, next_transaction, first_transaction):
    """ Insert a transaction into the linked list.

    Args:
        insert_transaction (Transaction): The Transaction to insert.
        prev_transaction (Transaction): The previous Transaction.
            The previous Transaction may be None if the insert Transaction is the new first item in the list.
        next_transaction (Transaction): The Transaction that will be after the insert Transaction.
        first_transaction (Transaction): The first Transaction in the list.  See Returns for more details.

    Returns:
        Transaction: The first Transaction.

        The first_transaction is modified only if it is changed in the insert process.
        Otherwise it the input parameter first_transaction is returned without modifications.
        This is a workaround for python pass-by-reference limitations.
    """

    # update data for previous or first transaction
    if prev_transaction:
        # a previous transaction exists
        # update the next pointer of the previous transaction
        prev_transaction.next = insert_transaction

    else:
        # previous transaction does not exist, therefore it is the new beginning of list
        first_transaction = insert_transaction

    # update data for next transaction
    insert_transaction.next = next_transaction

    return first_transaction


def output_append(output, n):
    """ Append a line to an output string.
    
    Args:
        output (str): The output string to append to. 
        n: A number.

    Returns:
        str: The output with appended object.
    """
    
    output += "{0:.2f}\n".format(n)
    return output


def process(in_file, out_file):
    """ Processes a stream of venmo payments saved to a text file.

    Saves an output file with a list of the median vertex degree of the current graph after processing each transaction
    present in the stream.

    Args:
        in_file: path to the input file
        out_file: path to desired output file
    """

    if not os.path.exists(in_file):
        raise Exception('Input file does not exist: ' + in_file)

    first_transaction = None  # store transactions as linked list for fast insertions and deletions
    window_end = None  # current end of 60 second window
    last_median = None  # last calculated median
    output = '' # string of output to save

    f = open(in_file)

    for line in f:
        # process a new transaction
        new_transaction = Transaction(line)

        if not window_end or new_transaction.time > window_end:
            # first transaction or new end of 60 second window
            window_end = new_transaction.time

        elif (window_end - new_transaction.time).seconds > 60:
            # transaction is prior to current 60 second window
            # do not add transaction to graph
            # print last known median
            output = output_append(output, last_median)
            # continue to next transaction immediately
            continue

        # iterate through linked list
        # along the way, do the following:
        # - delete transactions outside 60 second window
        # - insert the new transaction at the appropriate place
        # - tally up the number of transactions each person is involved with

        # initialize vertex degree counter with data from new_transaction
        payments = Counter()
        payments[new_transaction.actor] += 1
        payments[new_transaction.target] += 1

        # initialize values for iterating through previous transactions
        cur_transaction = first_transaction
        last_transaction = None
        inserted = False

        while cur_transaction:

            # check if transaction is inside window
            if (window_end - cur_transaction.time).seconds > 60:
                # transaction outside window
                # remove from list
                first_transaction = cur_transaction.next

            else:
                # transaction inside window
                # add vertex degree tally data
                payments[cur_transaction.target] += 1
                payments[cur_transaction.actor] += 1

                #  check if time is appropriate to insert new_transaction
                if not inserted and cur_transaction.time >= new_transaction.time:
                    # insert here to maintain order by time
                    first_transaction = insert(new_transaction, last_transaction, cur_transaction, first_transaction)

                    # note insertion
                    inserted = True

                # since the current transaction is valid, replace the last transaction with this transaction
                last_transaction = cur_transaction

            # increment to next transaction
            cur_transaction = cur_transaction.next

        # after reaching end of list, make sure new_transaction was inserted
        if not inserted:
            first_transaction = insert(new_transaction, last_transaction, cur_transaction, first_transaction)

        # calculated median vertex degree
        vertex_degrees = payments.most_common()
        n = len(vertex_degrees)
        halfway = n / 2
        if n % 2:
            # odd number of vertexes
            last_median = vertex_degrees[halfway][1]
        else:
            # even number of vertexes
            last_median = (vertex_degrees[halfway][1] + vertex_degrees[halfway - 1][1]) / 2.0

        output = output_append(output, last_median)

    # done reading transaction, save to file

    # ensure output directory exists
    folder = os.path.split(out_file)[0]
    if not os.path.exists(folder):
        # attempt to create output directory
        os.mkdirs(folder)

    # write file
    with open(out_file, 'w') as f:
        f.write(output)


if __name__ == "__main__":
    # receive arguments from command line and send to process function
    process(sys.argv[1], sys.argv[2])
