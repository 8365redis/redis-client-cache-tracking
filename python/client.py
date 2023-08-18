import redis
import argparse

from cct_prepare import add_list, create_index, generate_input
from cct_tests import unique_id_tracking_test_1

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

def flush_db():
    r.flushall()

def run_tests():
    data = add_test_data()
    unique_id_tracking_test_1(r,data)

def add_test_data():
    flush_db() # clean all db first
    create_index(r)
    data = generate_input(10)
    add_list(r, data)
    return data

def version():
    print("Version 1.0 - Python client to test CCT Redis Module")

def handle_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version' , default=False ,  action='store_true' , help="show version")
    parser.add_argument('-a', '--add' , default=False ,  action='store_true' , help="just add test data")
    parser.add_argument('-r', '--run' , default=False ,  action='store_true' , help="add the test data and run the tests")
    args = parser.parse_args()
    if args.version:
        version() 
    elif args.add:
        add_test_data()
    elif args.run:
        run_tests() 
    else:
        parser.print_usage()    

def main():
    handle_arguments()


if __name__ == "__main__":
    main()
