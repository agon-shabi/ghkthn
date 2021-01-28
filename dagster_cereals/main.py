from dagster import execute_pipeline, pipeline, solid

from hello_dagster import *

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    print_hi('PyCharm')
    execute_pipeline(hello_pipeline)   # Hello, dagster!