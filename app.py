import os
import gzip
import json
import hashlib

def hash_personal_info(info):
    # Use SHA256 hashing for anonymisation
    return hashlib.sha256(info.encode()).hexdigest()

def process_customer_dataset(customer_file_path, erasure_requests):
    customer_ids = []

    if not os.path.exists(customer_file_path):
        print(f"No customers file found at {customer_file_path}")
        return customer_ids

    try:
        with gzip.open(customer_file_path, 'rb') as customer_file:
            for line in customer_file:
                customer_data = json.loads(line)
                # Process the customer data here
                customer_id = customer_data.get('id')
                if customer_id:
                    customer_ids.append(customer_id)
                else:
                    print("Invalid customer data:", customer_data)
    except Exception as e:
        print(f"Error processing customers file: {e}")

    return customer_ids

def process_products_dataset(products_file_path):
    processed_count = 0
    invalid_count = 0
    sku_set = set()

    with gzip.open(products_file_path, 'rb') as products_file:
        for line in products_file:
            processed_count += 1
            product = json.loads(line)

            # Check for required fields
            required_fields = ['sku', 'name', 'price', 'category', 'popularity']
            if not all(field in product for field in required_fields):
                invalid_count += 1
                continue

            # Check uniqueness of SKU
            sku = product['sku']
            if sku in sku_set:
                invalid_count += 1
            else:
                sku_set.add(sku)

            # Check if price is valid
            try:
                price = float(product['price'])
                if price <= 0:
                    invalid_count += 1
            except ValueError:
                invalid_count += 1

            # Check if popularity is valid
            popularity = product['popularity']
            if popularity <= 0:
                invalid_count += 1

    print(f"Processed: {processed_count}, Invalid: {invalid_count}")
    return list(sku_set), processed_count

def process_transactions_dataset(transactions_file_path, customer_ids, product_skus):
    processed_count = 0
    invalid_count = 0

    try:
        with gzip.open(transactions_file_path, 'rb') as transactions_file:
            for line in transactions_file:
                processed_count += 1
                transaction = json.loads(line)

                # Check for required fields
                required_fields = ['transaction_id', 'transaction_time', 'customer_id', 'delivery_address', 'purchases']
                if not all(field in transaction for field in required_fields):
                    invalid_count += 1
                    continue

                # Check uniqueness of transaction id
                transaction_id = transaction['transaction_id']
                # Add logic to check uniqueness

                # Check if customer_id exists
                customer_id = transaction['customer_id']
                if customer_id not in customer_ids:
                    invalid_count += 1

                # Check if product skus exist
                purchases = transaction['purchases']
                if 'products' not in purchases:
                    invalid_count += 1
                    continue

                for product in purchases['products']:
                    if 'sku' not in product or product['sku'] not in product_skus:
                        invalid_count += 1

                # Check if total_cost matches
                total_cost = transaction['purchases']['total_cost']
                # Add logic to check total_cost
    except FileNotFoundError:
        print(f"No transactions file found at {transactions_file_path}")

    print(f"Processed: {processed_count}, Invalid: {invalid_count}")

def load_erasure_requests(erasure_requests_file_path):
    erasure_requests = {}

    try:
        with gzip.open(erasure_requests_file_path, 'rb') as erasure_requests_file:
            for line in erasure_requests_file:
                request = json.loads(line)
                if 'customer-id' in request and 'email' in request:
                    erasure_requests[request['customer-id']] = 'id'
                    erasure_requests[request['email']] = 'email'
                elif 'customer-id' in request:
                    erasure_requests[request['customer-id']] = 'id'
                elif 'email' in request:
                    erasure_requests[request['email']] = 'email'
    except FileNotFoundError:
        pass

    return erasure_requests

def write_processed_data(data, output_file_path):
    with open(output_file_path, 'w') as output_file:
        json.dump(data, output_file)

def main():
    # Path to the top-level folder
    root_folder_path = 'test-data'
    output_folder_path = 'processed-data'  # Specify the path to save processed data
    if not os.path.exists(output_folder_path):
        os.makedirs(output_folder_path)
    # Load erasure requests
    erasure_requests_file_path = os.path.join(root_folder_path, 'erasure-requests.json.gz')
    erasure_requests = load_erasure_requests(erasure_requests_file_path)

    # Accumulate processed data
    all_customer_ids = []
    all_product_skus = set()

    # Process datasets
    for root, dirs, files in os.walk(root_folder_path):
        for dir_name in dirs:
            if dir_name.startswith('date='):
                date_folder_path = os.path.join(root, dir_name)
                for _, hour_dirs, _ in os.walk(date_folder_path):
                    for hour_dir in hour_dirs:
                        if hour_dir.startswith('hour='):
                            hour_folder_path = os.path.join(date_folder_path, hour_dir)

                            # Process each dataset
                            customer_file_path = os.path.join(hour_folder_path, 'customers.json.gz')
                            products_file_path = os.path.join(hour_folder_path, 'products.json.gz')
                            transactions_file_path = os.path.join(hour_folder_path, 'transactions.json.gz')

                            product_skus = set()  # Initialize product_skus here

                            if os.path.exists(products_file_path):
                                product_skus, _ = process_products_dataset(products_file_path)
                                all_product_skus.update(product_skus)
                            else:
                                print(f"Products file not found at {products_file_path}")

                            if os.path.exists(customer_file_path):
                                customer_ids = process_customer_dataset(customer_file_path, erasure_requests)
                                all_customer_ids.extend(customer_ids)
                            else:
                                print(f"Customers file not found at {customer_file_path}")
                                customer_ids = []

                            if os.path.exists(transactions_file_path):
                                process_transactions_dataset(transactions_file_path, customer_ids, product_skus)
                            else:
                                print(f"Transactions file not found at {transactions_file_path}")

    # Write accumulated processed data to a single file
    processed_data = {
        'customer_ids': all_customer_ids,
        'product_skus': list(all_product_skus)
    }
    output_file_path = os.path.join(output_folder_path, 'processed_data.json')
    write_processed_data(processed_data, output_file_path)

if __name__ == "__main__":
    main()