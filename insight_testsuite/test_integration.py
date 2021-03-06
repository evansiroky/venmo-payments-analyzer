import os
import shutil
import sys
import unittest

from src.rolling_median import process


class TestMedianCalculation(unittest.TestCase):

    temp_folder = 'temp'

    def setUp(self):
        os.mkdir(self.temp_folder)

    def tearDown(self):
        shutil.rmtree(self.temp_folder)



    def test_rolling_median(self):
        """ Tests the rolling median script using provided test cases."""

        # gather tests
        test_data_dirs = './insight_testsuite/tests'
        for test_folder in os.listdir(test_data_dirs):
            in_filename = os.path.join(test_data_dirs, test_folder, 'venmo_input', 'venmo-trans.txt')
            out_filename = os.path.join(self.temp_folder, 'output.txt')
            expected_output_filename = os.path.join(test_data_dirs, test_folder, 'venmo_output', 'output.txt')

            process(in_filename, out_filename)

            actual_out_file = open(out_filename)
            expected_output_file = open(expected_output_filename)

            actual_lines = actual_out_file.readlines()
            expected_lines = expected_output_file.readlines()
            line_no = 0

            actual_out_file.close()
            expected_output_file.close()

            while True:
                try:
                    actual_line = actual_lines[line_no]
                except IndexError:
                    # reached end of actual data, expect should also throw same exception
                    try:
                        expected_line = expected_lines[line_no]
                        raise Exception('End of file found in expected output, but actual output still has more data.')
                    except IndexError:
                        pass

                    break

                try:
                    expected_line = expected_lines[line_no]
                except IndexError:
                    raise Exception('End of file found in actual output, but expected output still has more data.')

                assert expected_line.strip() == actual_line.strip(), \
                    'incorrect median on line {0} in {1}'.format(line_no, test_folder)

                line_no += 1
