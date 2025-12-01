import warnings
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
from utils.logger_helper import setup_logger

logger = setup_logger()

# avoid huge image DOS protection
PIL.Image.MAX_IMAGE_PIXELS = 933120000

# Silence the specific PIL TIFF metadata warning to keep logs clean
warnings.filterwarnings(
    "ignore",
    message=r"Metadata Warning, tag 33723 had too many entries.*",
    category=UserWarning,
    module=r"PIL.TiffImagePlugin"
)

NAMESPACE = {'ns': '*'}

# Constants
ACCEPTED_IMAGE_EXTS = ('.png', '.jpg', '.jpeg', '.tif', '.bmp', '.gif', '.heic')
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X x.y; rv:42.0) Gecko/20100101 Firefox/42.0'
}
MAX_RETRIES = 3
REQUEST_TIMEOUT = 3  # seconds for requests

# Paths
dataFolder = "/data/"
assetsFolder = "/assets"
logsFolder = "/logs/"
imagesFolder = os.path.join(assetsFolder, 'images')
pdfFolder = os.path.join(assetsFolder, 'pdfs')
os.makedirs(imagesFolder, exist_ok=True)
os.makedirs(pdfFolder, exist_ok=True)
os.makedirs(logsFolder, exist_ok=True)

# Argument parser
parser = argparse.ArgumentParser(description="Download and process images and PDFs from a CSV file.")
parser.add_argument('--input-file', required=True, help="The CSV file containing image and PDF URLs")
parser.add_argument('--metadata-dir', required=True, help="Directory where metadata.json is stored")
parser.add_argument('--offset', type=int, default=0, help="Offset for processing (default: 0)")
parser.add_argument('--limit', type=int, default=999999, help="Limit for processing (default: unlimited)")
args = parser.parse_args()

csvFile = os.path.join(dataFolder, args.input_file)
offset = args.offset
metadata_dir = args.metadata_dir
limit = args.limit
date = datetime.now(timezone.utc).strftime("%Y_%m_%d__%H_%M_%S")

metadata = ItemMetadata(metadata_dir)
session = requests.Session()
session.headers.update(HEADERS)


def download_image(_system_object_id, image_url, xml_filename, writer):
    if not image_url or not image_url.lower().endswith(ACCEPTED_IMAGE_EXTS):
        logger.warning("Skipping invalid image: %s %s", _system_object_id, image_url)
        return

    if image_url == metadata.getLatestImageDownloadUrlForFile(xml_filename):
        logger.debug("Image already downloaded for %s: %s", _system_object_id, image_url)
        return

    outputFile = os.path.join(imagesFolder, f'cms-{_system_object_id}.tif')

    try:
        attempts = 0
        r = None
        while attempts < MAX_RETRIES:
            attempts += 1
            try:
                r = session.get(image_url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                logger.debug("Request attempt %d failed for %s: %s", attempts, _system_object_id, e)
                time.sleep(1)
                continue

            content_type = r.headers.get('Content-Type', '')
            if 'image' in content_type.lower():
                break
            else:
                logger.debug("Attempt %d: unexpected Content-Type %s for %s", attempts, content_type, _system_object_id)
                time.sleep(1)

        if r is None or 'image' not in r.headers.get('Content-Type', '').lower():
            logger.warning("Could not download image for %s after %d attempts: %s", _system_object_id, attempts, image_url)
            return

        if image_url.lower().endswith('.heic'):
            heif_file = pillow_heif.read_heif(BytesIO(r.content))
            img = Image.frombytes(heif_file.mode, heif_file.size, heif_file.data, "raw", heif_file.mode, heif_file.stride)
        else:
            img = Image.open(BytesIO(r.content))

        img.save(outputFile, 'TIFF')
        writer.writerow([f'cms-{_system_object_id}.tif', outputFile])
        metadata.setLatestImageDownloadUrlForFile(xml_filename, image_url)
        logger.debug("Saved image for %s to %s", _system_object_id, outputFile)

    except Exception as e:
        logger.error("Error processing image for %s: %s", _system_object_id, e, exc_info=True)


def download_pdf(_system_object_id, pdf_url, xml_filename, writer):
    if not pdf_url or not pdf_url.strip().lower().endswith(".pdf"):
        return

    if pdf_url == metadata.getLatestPdfDownloadUrlForFile(xml_filename):
        logger.debug("PDF already downloaded for %s", _system_object_id)
        return

    output_pdf_File = os.path.join(pdfFolder, f'cms-{_system_object_id}.pdf')

    try:
        attempts = 0
        r = None
        while attempts < MAX_RETRIES:
            attempts += 1
            try:
                r = session.get(pdf_url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                logger.debug("PDF request attempt %d failed for %s: %s", attempts, _system_object_id, e)
                time.sleep(1)
                continue

            content_type = r.headers.get('Content-Type', '')
            if 'application/pdf' in content_type.lower():
                break
            else:
                logger.debug("Attempt %d: unexpected Content-Type %s for PDF %s", attempts, content_type, _system_object_id)
                time.sleep(1)

        if r is None or 'application/pdf' not in r.headers.get('Content-Type', '').lower():
            logger.warning("Could not download PDF for %s after %d attempts: %s", _system_object_id, attempts, pdf_url)
            return

        with open(output_pdf_File, 'wb') as f:
            f.write(r.content)

        writer.writerow([f'cms-{_system_object_id}.pdf', output_pdf_File])
        metadata.setLatestPdfDownloadUrlForFile(xml_filename, pdf_url)
        logger.debug("Saved PDF for %s to %s", _system_object_id, output_pdf_File)

    except Exception as e:
        logger.error("Error downloading PDF for %s: %s", _system_object_id, e, exc_info=True)


# Process CSV list
with open(csvFile, 'r') as f:
    data = list(csv.DictReader(f))

logger.info(f"The number of entries in {args.input_file} is {len(data)}")

with open(f'{logsFolder}to_db_{date}.csv', 'w', newline='', encoding='utf-8') as img_csv_file, \
     open(f'{logsFolder}to_db_pdf_{date}.csv', 'w', newline='', encoding='utf-8') as pdf_csv_file:

    image_writer = csv.writer(img_csv_file)
    pdf_writer = csv.writer(pdf_csv_file)
    for row in tqdm(data[offset:offset + limit]):
        _system_object_id = row['_system_object_id']
        xml_filename = row["filename"]
        image_url = row['image_url']
        pdf_url = row['pdf_url']
        
        download_image(_system_object_id, image_url, xml_filename, image_writer)
        download_pdf(_system_object_id, pdf_url, xml_filename, pdf_writer)

logger.info(f"Download complete. Lists of downloaded files saved to {logsFolder}to_db_{date}.csv and {logsFolder}to_db_pdf_{date}.csv")
