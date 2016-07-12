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

        if self.target == '' or self.actor == '':
            raise ValueError


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
        print('Input file does not exist!')
        return

    first_transaction = None  # store transactions as linked list for fast insertions and deletions
    window_end = None  # current end of 60 second window
    last_median = None  # last calculated median
    output = '' # string of output to save

    f = open(in_file)

    for line in f:
        # process a new transaction
        try:
            new_transaction = Transaction(line)
        except ValueError:
            # invalid input line, skip current line
            continue
        except KeyError:
            # one or more json fields are missing, skip current line
            continue

        if not window_end or new_transaction.time > window_end:
            # first transaction or new end of 60 second window
            window_end = new_transaction.time

        elif (window_end - new_transaction.time).seconds > 60:
            # transaction is prior to current 60 second window
            # do not add transaction to graph
            # output last known median
            output = output_append(output, last_median)
            # continue to next transaction immediately
            continue

        # iterate through linked list
        # along the way, do the following:
        # - delete transactions outside 60 second window
        # - insert the new transaction at the appropriate place
        # - tally up the number of transactions each person is involved with
        # - make sure the transaction is not duplicate (and keep only the most recent duplicate if it is)

        # initialize vertex degree counter with data from new_transaction
        payments = Counter()
        payments[new_transaction.actor] += 1
        payments[new_transaction.target] += 1

        # initialize values for iterating through previous transactions
        cur_transaction = first_transaction
        last_transaction = None

        # initialize variables related to inserting and duplicate handling
        duplicate_found = False
        should_insert = True
        found_insertion_point = False
        insert_before = None  # placeholder for transaction to insert after
        insert_after = None  # placeholder for transaction to insert before

        while cur_transaction:

            # check if transaction is inside window
            if (window_end - cur_transaction.time).seconds > 60:
                # transaction outside window
                # remove from list
                first_transaction = cur_transaction.next

            else:
                # transaction inside window

                # determine if current transaction involves target-actor combo of new transaction
                if (cur_transaction.target == new_transaction.target and \
                        cur_transaction.actor == new_transaction.actor) or \
                    (cur_transaction.actor == new_transaction.target and \
                        cur_transaction.target == new_transaction.actor):
                    # transaction connection already present in graph
                    duplicate_found = True

                    # keep transaction if it is later than new transaction
                    if cur_transaction.time >= new_transaction.time:
                        # current transaction should stay in graph
                        # no insertion necessary, exit loop
                        should_insert = False
                        break
                    else:
                        # current transaction occurred prior to new transaction
                        # remove current transaction from linked list
                        cur_transaction = cur_transaction.next

                        # update point of last or first transaction
                        if last_transaction:
                            last_transaction.next = cur_transaction
                        else:
                            first_transaction = cur_transaction

                        # handle edge case of removing transaction at end of list
                        if not cur_transaction:
                            insert_before = last_transaction
                            break

                        # continue to next step in loop since update step has been performed
                        continue

                else:

                    # transaction connection not duplicate
                    # add vertex degree tally data
                    payments[cur_transaction.target] += 1
                    payments[cur_transaction.actor] += 1

                    #  check if time is appropriate to insert new_transaction
                    if not found_insertion_point and cur_transaction.time >= new_transaction.time:
                        # note insert point (a duplicate transaction might appear later, so don't insert yet)
                        insert_before = last_transaction
                        insert_after = cur_transaction

                        # note insertion
                        found_insertion_point = True

                        # end loop early if duplicate already found in graph
                        if duplicate_found:
                            break

                # since the current transaction is valid, replace the last transaction with this transaction
                last_transaction = cur_transaction

            # increment to next transaction
            cur_transaction = cur_transaction.next

        # after reaching end of list, insert transaction if needed
        if should_insert:
            # handle case when appending
            insert_before = insert_before if found_insertion_point else last_transaction
            # put into list
            first_transaction = insert(new_transaction, insert_before, insert_after, first_transaction)

        # only recalculate median if duplicate not found
        if not duplicate_found:
            # calculated median vertex degree
            vertex_degrees = payments.most_common()
            n = len(vertex_degrees)
            halfway = int(n / 2)
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
    if len(sys.argv) != 3:
        print('Invalid command!')
        print('Usage: rolling_median.py input_path output_path')
    else:
        process(sys.argv[1], sys.argv[2])
