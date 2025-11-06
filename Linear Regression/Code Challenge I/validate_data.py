# Define the test functions

import pandas as pd
import os
import pytest

# Define the test functions

def test_read_field_DataFrame_shape():
    field_df = pd.read_csv('sampled_field_df.csv')
    assert field_df.shape == (5654, 19)

def test_field_DataFrame_columns():
    field_df = pd.read_csv('sampled_field_df.csv')
    expected_columns = ['Field_ID', 'Elevation', 'Latitude', 'Longitude',
                       'Location', 'Slope', 'Rainfall', 'Min_temperature_C',
                        'Max_temperature_C', 'Ave_temps', 'Soil_fertility']
def test_field_DataFrame_non_negative_elevation():
    field_df = pd.read_csv('sampled_field_df.csv')
    assert (field_df['Elevation'] >= 0).all()

def test_crop_types_are_valid():
    field_df = pd.read_csv('sampled_field_df.csv')
    valid_crop_type = ['cassava', 'wheat', 'tea', 'potato', 'banana', 'coffee', 'maize', 'rice','cassava ','wheat ','tea ']  # Define your valid crop types here
    assert field_df['Crop_type'].isin(valid_crop_type).all()

# # Run the tests
# if __name__ == "__main__":
#     pytest.main(['-v', 'validate_data.py'])

#     # Define the file paths
#     field_csv_path = 'sampled_field_df.csv'

#     # Delete sampled_field_df.csv if it exists
#     if os.path.exists(field_csv_path):
#         os.remove(field_csv_path)
#         print(f"Deleted {field_csv_path}")
#     else:
#         print(f"{field_csv_path} does not exist.")