import redis
import argparse

from python.cct_test import unique_id_tracking_test_1

r = redis.Redis(host='localhost', port=6379, decode_responses=True)


def run_tests():
    unique_id_tracking_test_1()


def version():
    print("Version 1.0 - Python client to test CCT Redis Module")

def handle_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('-v', '--version' , default=False ,  action='store_true' , help="show version")
    parser.add_argument('-r', '--run' , default=False ,  action='store_true' , help="add the test data and run the tests")
    args = parser.parse_args()
    if args.version:
        version() 
    elif args.run:
        run_tests() 
    else:
        parser.print_usage()    

def main():
    handle_arguments()


if __name__ == "__main__":
    main()
