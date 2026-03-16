import os
import re
import uuid
import shutil
import warnings
import boto3
import pandas as pd
import requests
import zipfile
from flask import Flask, render_template, request, send_file
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import quote
from PIL import Image, ImageFile
import piexif
import pillow_heif

# ================= CONFIG =================
BUCKET_NAME = "order-management-resources"
REGION = "ap-south-1"
CLOUDFRONT_URL = "https://d1fnp4flbue4ed.cloudfront.net"

s3 = boto3.client("s3", region_name=REGION)

app = Flask(__name__)
pillow_heif.register_heif_opener()
ImageFile.LOAD_TRUNCATED_IMAGES = True
warnings.simplefilter("ignore")

MAX_SIDE = 6000
MAX_SIZE_KB = 800
MAX_COLUMNS = 20
VALID_IMAGE_EXTS = (".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff", ".heic", ".heif")

os.makedirs("temp", exist_ok=True)

# ================= UTIL =================
def sanitize_name(name):
    return re.sub(r'[<>:"/\\|?*]', '_', str(name).strip())

def _safe_download_base(name: str) -> str:
    """
    Fixes the xlsx_ issue by removing:
    - trailing spaces
    - NBSP (\u00A0)
    - control chars
    - trailing dots/spaces (Windows invalid)
    - forbidden filename chars
    """
    s = str(name)

    # Remove control characters anywhere
    s = re.sub(r"[\x00-\x1f\x7f]", "", s)

    # Normalize NBSP and other odd spaces to normal space
    s = s.replace("\u00A0", " ").replace("\u2007", " ").replace("\u202F", " ")

    # Strip and remove trailing dots/spaces (Windows invalid)
    s = s.strip().rstrip(" .")

    # Remove forbidden filename characters
    s = re.sub(r'[<>:"/\\|?*]+', "_", s)

    # If empty, fallback
    if not s:
        s = "output"

    return s

def is_valid_image_file(filename):
    if not filename:
        return False

    lower_name = filename.lower()

    if lower_name == ".ds_store":
        return False
    if lower_name.startswith("._"):
        return False
    if lower_name.startswith("."):
        return False

    return lower_name.endswith(VALID_IMAGE_EXTS)

def compress_image(img, output_path):
    clean_exif = {"0th": {}, "Exif": {}, "GPS": {}, "Interop": {}, "1st": {}, "thumbnail": None}
    exif_bytes = piexif.dump(clean_exif)

    quality = 90
    while quality >= 20:
        img.save(output_path, "JPEG", quality=quality, optimize=True, exif=exif_bytes)
        if os.path.getsize(output_path) / 1024 <= MAX_SIZE_KB:
            return
        quality -= 10

def convert_image(input_path, output_path):
    with Image.open(input_path) as img:
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")

        if max(img.size) > MAX_SIDE:
            img.thumbnail((MAX_SIDE, MAX_SIDE), Image.LANCZOS)

        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        compress_image(img, output_path)

def upload_folder_to_s3(local_folder, s3_prefix):
    for root, dirs, files in os.walk(local_folder):
        # Skip macOS metadata folders
        dirs[:] = [d for d in dirs if d != "__MACOSX" and not d.startswith(".")]

        for file in files:
            if not is_valid_image_file(file):
                continue

            local_path = os.path.join(root, file)

            # Skip empty files
            if not os.path.isfile(local_path) or os.path.getsize(local_path) == 0:
                continue

            relative_path = os.path.relpath(local_path, local_folder)
            s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")

            s3.upload_file(
                local_path,
                BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    "ContentType": "image/jpeg",
                    "ContentDisposition": "inline"
                }
            )

# ================= ROUTES =================
@app.route("/")
def home():
    return render_template("index.html")

@app.route("/process", methods=["POST"])
def process():

    excel_files = request.files.getlist("files") or request.files.getlist("file")
    zip_file = request.files.get("zip_file")

    if not excel_files and not zip_file:
        return "No file uploaded", 400

    final_data = []

    # ================= ZIP MODE =================
    if zip_file and zip_file.filename != "":
        job_id = str(uuid.uuid4())
        sheet_name = os.path.splitext(zip_file.filename)[0]

        base_dir = f"temp/{job_id}"
        extract_dir = os.path.join(base_dir, "imgs_final")

        os.makedirs(extract_dir, exist_ok=True)

        zip_path = os.path.join(base_dir, zip_file.filename)
        zip_file.save(zip_path)

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)

        s3_prefix = f"BAU/imgs_final/{job_id}"
        upload_folder_to_s3(extract_dir, s3_prefix)

        base_url = f"{CLOUDFRONT_URL}/{s3_prefix}"

        for root, dirs, files in os.walk(extract_dir):
            # Skip macOS metadata folders
            dirs[:] = [d for d in dirs if d != "__MACOSX" and not d.startswith(".")]

            # Keep only valid image files
            files = [f for f in files if is_valid_image_file(f)]

            if not files:
                continue

            rel_path = os.path.relpath(root, extract_dir).replace("\\", "/")

            if rel_path == ".":
                sku = os.path.splitext(files[0])[0]
            else:
                sku = rel_path.split("/")[-1]

            row_data = {"SHEET": sheet_name, "sku": sku}

            for i in range(1, MAX_COLUMNS + 1):
                row_data[f"path {i}"] = ""

            for idx, file in enumerate(sorted(files)[:MAX_COLUMNS]):
                if rel_path == ".":
                    full_url = f"{base_url}/{quote(file, safe='')}"
                else:
                    full_url = f"{base_url}/{quote(rel_path, safe='')}/{quote(file, safe='')}"

                row_data[f"path {idx+1}"] = full_url

            final_data.append(row_data)

        shutil.rmtree(base_dir, ignore_errors=True)

    # ================= EXCEL MODE =================
    for uploaded_file in excel_files:
        if uploaded_file.filename == "":
            continue

        job_id = str(uuid.uuid4())
        sheet_name = os.path.splitext(uploaded_file.filename)[0]

        base_dir = f"temp/{job_id}"
        imgs_dir = os.path.join(base_dir, "imgs")
        imgs_final = os.path.join(base_dir, "imgs_final")

        os.makedirs(imgs_dir, exist_ok=True)

        input_path = os.path.join(base_dir, uploaded_file.filename)
        uploaded_file.save(input_path)

        df = pd.read_excel(input_path)
        records = df.to_dict("records")

        # Download Images
        for row in records:
            sku = sanitize_name(row.get("sku", "unknown"))
            sku_folder = os.path.join(imgs_dir, sku)
            os.makedirs(sku_folder, exist_ok=True)

            counter = 1
            for i in range(1, 21):
                url = row.get(f"img_url {i}")
                if not url:
                    continue
                try:
                    r = requests.get(url, timeout=60)
                    r.raise_for_status()

                    img_path = os.path.join(sku_folder, f"{sku}_{counter}.jpg")
                    with open(img_path, "wb") as f:
                        f.write(r.content)
                    counter += 1
                except:
                    pass

        # Convert
        tasks = []
        for root, _, files in os.walk(imgs_dir):
            for file in files:
                if not is_valid_image_file(file):
                    continue

                input_path = os.path.join(root, file)
                rel = os.path.relpath(input_path, imgs_dir)
                output_path = os.path.join(imgs_final, os.path.splitext(rel)[0] + ".jpg")
                tasks.append((input_path, output_path))

        os.makedirs(imgs_final, exist_ok=True)

        with ThreadPoolExecutor(max_workers=16) as executor:
            executor.map(lambda t: convert_image(*t), tasks)

        # Upload
        s3_prefix = f"BAU/imgs_final/{job_id}"
        upload_folder_to_s3(imgs_final, s3_prefix)

        base_url = f"{CLOUDFRONT_URL}/{s3_prefix}"

        for root, _, files in os.walk(imgs_final):
            files = [f for f in files if is_valid_image_file(f)]

            if not files:
                continue

            rel_path = os.path.relpath(root, imgs_final).replace("\\", "/")
            sku = rel_path.split("/")[-1]

            row_data = {"SHEET": sheet_name, "sku": sku}

            for i in range(1, MAX_COLUMNS + 1):
                row_data[f"path {i}"] = ""

            for idx, file in enumerate(sorted(files)[:MAX_COLUMNS]):
                full_url = f"{base_url}/{quote(rel_path, safe='')}/{quote(file, safe='')}"
                row_data[f"path {idx+1}"] = full_url

            final_data.append(row_data)

        shutil.rmtree(base_dir, ignore_errors=True)

    # ================= FINAL EXCEL =================
    columns = ["SHEET", "sku"] + [f"path {i}" for i in range(1, MAX_COLUMNS + 1)]

    if excel_files:
        base_filename = os.path.splitext(excel_files[0].filename)[0]
    elif zip_file:
        base_filename = os.path.splitext(zip_file.filename)[0]
    else:
        base_filename = "output"

    # ✅ FIX: aggressive cleanup to stop Chrome saving as .xlsx_
    base_filename = _safe_download_base(base_filename)

    output_filename = f"{base_filename}_whitelisted_links.xlsx"
    output_excel = f"temp/{output_filename}"

    pd.DataFrame(final_data, columns=columns).to_excel(output_excel, index=False)

    response = send_file(
        output_excel,
        as_attachment=True,
        download_name=output_filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # ✅ Strong Content-Disposition
    response.headers["Content-Disposition"] = f'attachment; filename="{output_filename}"'
    return response

if __name__ == "__main__":
    app.run(debug=True, threaded=True)