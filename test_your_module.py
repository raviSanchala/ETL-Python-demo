import unittest
import os
import shutil
import json
import gzip
from tempfile import TemporaryDirectory
import importlib.util

from app import (
    process_customer_dataset,
    process_products_dataset,
    process_transactions_dataset,
    load_erasure_requests
)

# Dynamically import functions from app.py
spec = importlib.util.spec_from_file_location("app", "app.py")
app = importlib.util.module_from_spec(spec)
spec.loader.exec_module(app)

class TestProcessCustomerDataset(unittest.TestCase):
    def test_valid_customer_file(self):
        # Create a temporary directory
        with TemporaryDirectory() as tmpdirname:
            # Create a valid customers file
            customer_data = [{'id': '1'}, {'id': '2'}, {'id': '3'}]
            file_path = os.path.join(tmpdirname, 'customers.json.gz')
            with gzip.open(file_path, 'wt') as f:
                for data in customer_data:
                    f.write(json.dumps(data) + '\n')

            # Test the function
            customer_ids = process_customer_dataset(file_path, {})
            self.assertEqual(customer_ids, ['1', '2', '3'])

    def test_invalid_customer_file(self):
        # Create a temporary directory
        with TemporaryDirectory() as tmpdirname:
            # Create an empty customers file
            file_path = os.path.join(tmpdirname, 'customers.json.gz')
            open(file_path, 'w').close()

            # Test the function
            customer_ids = process_customer_dataset(file_path, {})
            self.assertEqual(customer_ids, [])

class TestProcessProductsDataset(unittest.TestCase):
    def test_valid_products_file(self):
        # Create a temporary directory
        with TemporaryDirectory() as tmpdirname:
            # Create a valid products file
            product_data = [
                {"sku": 53248, "name": "whpVcnUvCL", "price": "16.99", "category": "misc", "popularity": 0.8180098598621565},
                {"sku": 12289, "name": "kXWUrMALI", "price": "41.00", "category": "vitamin", "popularity": 0.4485113095049037},
                {"sku": 34821, "name": "KEaVsFhXYMuG", "price": "5.98", "category": "vitamin", "popularity": 0.5260703074826073}
            ]
            file_path = os.path.join(tmpdirname, 'products.json.gz')
            with gzip.open(file_path, 'wt') as f:
                for data in product_data:
                    f.write(json.dumps(data) + '\n')

            # Test the function
            sku_set, processed_count = app.process_products_dataset(file_path)
            self.assertEqual(sku_set, {53248, 12289, 34821})
            self.assertEqual(processed_count, 3)

if __name__ == '__main__':
    unittest.main()

if __name__ == '__main__':
    unittest.main()