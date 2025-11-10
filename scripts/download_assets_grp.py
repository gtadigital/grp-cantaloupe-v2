import csv
import requests
import time
import os
from datetime import datetime, timezone
import argparse
from io import BytesIO
import pillow_heif
from PIL import Image
import PIL
from tqdm import tqdm
from lib.Metadata import ItemMetadata

PIL.Image.MAX_IMAGE_PIXELS = 933120000

NAMESPACE = {'ns': '*'}

# Constants
ACCEPTED_IMAGE_EXTS = ('.png', '.jpg', '.jpeg', '.tif', '.bmp', '.gif', '.heic')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
}
MAX_RETRIES = 3

# Paths
dataFolder = "/data/"
assetsFolder = "/assets"
imagesFolder = os.path.join(assetsFolder, 'images')
pdfFolder = os.path.join(assetsFolder, 'pdfs')
os.makedirs(imagesFolder, exist_ok=True)
os.makedirs(pdfFolder, exist_ok=True)

# Argument parser
parser = argparse.ArgumentParser(description="Download and process images and PDFs from a CSV file.")
parser.add_argument('--input-file', required=True, help="The CSV file containing image and PDF URLs")
parser.add_argument('--offset', type=int, default=0, help="Offset for processing (default: 0)")
parser.add_argument('--limit', type=int, default=999999, help="Limit for processing (default: unlimited)")
args = parser.parse_args()

csvFile = os.path.join(dataFolder, args.input_file)
offset = args.offset
limit = args.limit
date = datetime.now(timezone.utc).strftime("%Y_%m_%d__%H_%M_%S")

metadata = ItemMetadata("/data/source")

def download_image(_system_object_id, image_url, xml_filename, writer):
    if not image_url.lower().endswith(ACCEPTED_IMAGE_EXTS):
        print(_system_object_id, image_url, "is not a valid image")
        return

    if image_url == metadata.getLatestImageDownloadUrlForFile(xml_filename):
        print("Image already downloaded", _system_object_id, image_url)
        return

    outputFile = os.path.join(imagesFolder, f'cms-{_system_object_id}.tif')

    try:
        r = requests.get(image_url, allow_redirects=True, headers=HEADERS)
        retries = 1
        while 'image' not in r.headers.get('Content-Type', '') and retries <= MAX_RETRIES:
            time.sleep(1)
            r = requests.get(image_url, allow_redirects=True, headers=HEADERS)
            retries += 1

        if retries >= MAX_RETRIES:
            print("Could not download", _system_object_id, image_url)
            return

        if image_url.lower().endswith('.heic'):
            heif_file = pillow_heif.read_heif(BytesIO(r.content))
            img = Image.frombytes(heif_file.mode, heif_file.size, heif_file.data, "raw", heif_file.mode, heif_file.stride)
        else:
            img = Image.open(BytesIO(r.content))

        img.save(outputFile, 'TIFF')
        writer.writerow([f'cms-{_system_object_id}.tif', outputFile])
        metadata.setLatestImageDownloadUrlForFile(xml_filename, image_url)

    except Exception as e:
        print(f"Error processing image for {_system_object_id}: {e}")


def download_pdf(_system_object_id, pdf_url, xml_filename, writer):
    if not pdf_url or not pdf_url.strip().endswith(".pdf"):
        return

    if pdf_url == metadata.getLatestPdfDownloadUrlForFile(xml_filename):
        print("PDF already downloaded", _system_object_id)
        return

    output_pdf_File = os.path.join(pdfFolder, f'cms-{_system_object_id}.pdf')

    try:
        r = requests.get(pdf_url, allow_redirects=True, headers=HEADERS)
        retries = 1
        while 'application/pdf' not in r.headers.get('Content-Type', '') and retries <= MAX_RETRIES:
            time.sleep(1)
            r = requests.get(pdf_url, allow_redirects=True, headers=HEADERS)
            retries += 1

        if retries >= MAX_RETRIES:
            print(f"Could not download PDF for {_system_object_id}: {pdf_url}")
            return

        with open(output_pdf_File, 'wb') as f:
            f.write(r.content)

        writer.writerow([f'cms-{_system_object_id}.pdf', output_pdf_File])
        metadata.setLatestPdfDownloadUrlForFile(xml_filename, pdf_url)
        # print(f"Downloaded and saved PDF cms-{_system_object_id}.pdf")

    except Exception as e:
        print(f"Error downloading PDF for {_system_object_id}: {e}")


# Process CSV list
with open(csvFile, 'r') as f:
    data = list(csv.DictReader(f))

print(f"The number of entries in {args.input_file} is {len(data)}")

with open(f'to_db_{date}.csv', 'w', newline='', encoding='utf-8') as img_csv_file, \
     open(f'to_db_pdf_{date}.csv', 'w', newline='', encoding='utf-8') as pdf_csv_file:

    image_writer = csv.writer(img_csv_file)
    pdf_writer = csv.writer(pdf_csv_file)
    for row in tqdm(data[offset:offset + limit]):
        _system_object_id = row['_system_object_id']
        xml_filename = row["filename"]
        image_url = row['image_url']
        pdf_url = row['pdf_url']
        
        download_image(_system_object_id, image_url, xml_filename, image_writer)
        download_pdf(_system_object_id, pdf_url, xml_filename, pdf_writer)

print(f"Download complete. Lists of downloaded files saved to to_db_{date}.csv and to_db_pdf_{date}.csv")
