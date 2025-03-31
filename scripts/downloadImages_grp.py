import csv
import requests
import sys
import time
import os
import datetime
import argparse
import PIL
from PIL import Image
PIL.Image.MAX_IMAGE_PIXELS = 933120000
from io import BytesIO
from tqdm import tqdm
from lib.Metadata import ItemMetadata

# Set up argument parser
parser = argparse.ArgumentParser(description="Download and process images from a CSV file.")
parser.add_argument('--input-file', required=True, help="The CSV file containing image URLs")
parser.add_argument('--offset', type=int, default=0, help="Offset for image processing (default: 0)")
parser.add_argument('--limit', type=int, default=999999, help="Limit for image processing (default: unlimited)")

args = parser.parse_args()

# Assign variables from parsed arguments
csvFileName = args.input_file  # Image file CSV name
csvFolder = "/data/"
csvFile = os.path.join(csvFolder, csvFileName)
offset = args.offset
limit = args.limit

metadata = ItemMetadata("/data/source")

# Accepted image file extensions
accepted_exts = ('.png', '.jpg', '.jpeg', '.tif', '.bmp', '.gif')

# Parent directory where images will be saved
parentFolder = '/images'

# Last part to be removed (only for debugging)
date = str(datetime.date.today().strftime('%Y_%m_%d')) + ""

maxRetries = 3

data = []

# Read CSV file
with open(csvFile, 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        data.append(row)

print("The number of entries in " + csvFileName + " is", len(data))  # Number of entries in the CSV

toDBFile = 'to_db_%s.csv' % (date)

with open(toDBFile, 'w') as g:
    writer = csv.writer(g)

    for row in tqdm(data[offset:offset + limit]):
        _id = row['filename'].rsplit('/', 1)[-1]  # Takes the element after the last slash
        print(_id)
        csv_url = row['image_url']
        xml_filename = row["filename"]
        
        latestImageDownloadUrl = metadata.getLatestImageDownloadUrlForFile(xml_filename)

        directory = os.path.join(parentFolder)
        os.makedirs(directory, exist_ok=True)

        outputFile = '%s/%s.tif' % (directory, _id)
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
        }

        # Check if the file is an image
        if not csv_url.lower().endswith(accepted_exts):
            print(_id, csv_url, "is not an image")
        
        elif (csv_url != latestImageDownloadUrl):
            r = requests.get(csv_url, allow_redirects=True, headers=headers)
            retries = 1
            while 'image' not in r.headers['Content-Type'] and retries <= maxRetries:
                # Try again if no image comes back
                time.sleep(1)
                r = requests.get(csv_url, allow_redirects=True, headers=headers)
                retries += 1

            if retries >= maxRetries:
                print("Could not download", _id, csv_url)

            else:
                img = Image.open(BytesIO(r.content))
                img.save(outputFile, 'TIFF')

                line = [_id + '.tif', directory + '/' + _id + '.tif']
                writer.writerow(line)
                metadata.setLatestImageDownloadUrlForFile(xml_filename, csv_url)
