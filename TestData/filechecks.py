def test_DQ_Sales_data_File_Availability():
    assert check_file_exists("TestData/sales_data1.csv"),"Sales_data.csv file doesn't exist in the source path"

def test_DQ_Sales_data_File_is_Blank():
    assert not check_file_empty("TestData/sales_data1.csv"),"Sales_data.csv file is empty"


def check_file_exists(file_path):
    """Check if the file exists at the given path."""
    return os.path.isfile(file_path)

def check_file_empty(file_path):
    """Check if the file is empty."""
    return os.path.getsize(file_path) == 0