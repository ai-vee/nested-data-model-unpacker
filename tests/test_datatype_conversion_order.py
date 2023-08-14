# test_main.py
import unittest
import datatype_conversion_order as DC

class TestDataTypeConversion(unittest.TestCase):
    def test_get_highest_order_dtype(self):
        test_cases = [
            (['varchar', 'double', 'int'], 'varchar')
            ,(['double', 'bool'], 'varchar')
            ,(['double', 'int'], 'double')
            ,(['float'], 'float')
            ,(['array'], 'array')
            ,(['null_type'], 'variant')
        ]
        
        for dtypes, expected_highest_dtype in test_cases:
            with self.subTest(dtypes = dtypes):
                self.assertEqual(DC.get_highest_order_dtype(*dtypes), expected_highest_dtype)

if __name__ == '__main__':
    unittest.main() 