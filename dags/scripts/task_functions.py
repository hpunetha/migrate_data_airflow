# Define the task functions in a separate script for reusability and modularity

def print_hello():
    # Function to print hello message
    print("********* Hello, world!")

def task_1():
    # Function to print task 1 executed message
    print("********* Task 1 executed")

def task_2():
    # Function to print task 2 executed message
    print("********* Task 2 executed")

def task_3():
    # Function to print task 3 executed message on task 2 failure
    print("********* Task 2 failed, executing Task 3")

def extract_data():
    # Function to simulate data extraction
    print("********* Extracting data")

def transform_data():
    # Function to simulate data transformation
    print("********* Transforming data")

def load_data():
    # Function to simulate data loading
    print("********* Loading data")

def send_success_email():
    # Function to simulate sending a success email
    print("********* Sending success email")

def send_failure_email():
    # Function to simulate sending a failure email
    print("********* Sending failure email")
