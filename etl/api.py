import csv
import arcpy
import requests
from config import set_path


class SpatialEtl:
    def __init__(self, config_dict):
        self.config_dict = config_dict

    def extract(self):
        print(f"Extracting from {self.config_dict['gsheet_url']} to {self.config_dict['etl_dir']}")

    def transform(self):
        print(f"Transforming {self.config_dict['format']}")

    def load(self):
        print(f"Loading data into {self.config_dict['gdb_dir']}")


class GSheetsEtl(SpatialEtl):
    """
    GSheetsEtl performs an extract, transform and load process using a URL to a google spreadsheet.
    The spreadsheet must contain an address and zipcode column.

    Parameters:
        config_dict (dictionary): A dictionary containing all the key value pairs from the config yaml.
    """

    def __init__(self, config_dict):
        super().__init__(config_dict)
        self.s = requests.Session()

    def extract(self):
        # Get data
        arcpy.AddMessage('Extracting addresses from google spreadsheet')
        r = self.s.get(self.config_dict.get('gsheet_url'))
        arcpy.AddMessage(f'HTTP Response: {r.status_code} \n {r.content} \n')
        r.encoding = 'utf-8'
        data = r.text

        # Write data to csv
        csv_path = set_path(self.config_dict['etl_dir'], 'addresses.csv')
        arcpy.AddMessage(f'Writing data to addresses.csv in {csv_path}\n')
        with open(csv_path, 'w', encoding='utf-8') as f:
            f.write(data)

    def transform(self):
        arcpy.AddMessage('Transforming addresses using geocoder')

        # Create transformed csv with X, Y, and Type for headers
        csv_path = set_path(self.config_dict['etl_dir'], 'addresses.csv')
        new_csv_path = set_path(self.config_dict['etl_dir'], 'new_addresses.csv')
        with open(new_csv_path, 'w', newline='', encoding='utf-8') as f:
            fieldnames = ['X', 'Y', 'Type']
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            with open(csv_path, 'r') as address_reader:
                csv_dict = csv.DictReader(address_reader, delimiter=',')
                for row in csv_dict:
                    address = row['Address']
                    arcpy.AddMessage(f'geocoding {address}')
                    url = f"{self.config_dict['geocoder_prefix_url']}{address}{self.config_dict['geocoder_suffix_url']}"
                    r = self.s.get(url)
                    resp_dict = r.json()
                    x = resp_dict['result']['addressMatches'][0]['coordinates']['x']
                    y = resp_dict['result']['addressMatches'][0]['coordinates']['y']
                    row_dict = {'X': x, 'Y': y, 'Type': 'Residential'}
                    arcpy.AddMessage(f'Writing row to new_addresses.csv: {row_dict}')
                    writer.writerow(row_dict)

    def load(self):
        arcpy.AddMessage('\nCreating a point feature class from input table\n')

        # Setup local variables
        in_table = set_path(self.config_dict['etl_dir'], 'new_addresses.csv')
        out_feature_class = 'avoid_points'
        x_coords = 'X'
        y_coords = 'Y'

        # Create XY event layer
        arcpy.management.XYTableToPoint(in_table, out_feature_class, x_coords, y_coords)

        arcpy.AddMessage(
            f'\nFeature class avoid_points created with {arcpy.GetCount_management(out_feature_class)} rows.')

    def process(self):
        self.extract()
        self.transform()
        self.load()
