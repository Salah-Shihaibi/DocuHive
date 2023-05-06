import os

import numpy as np
import pdf2image
import cv2
import fitz
import pytest

from DocuHive.backend_utils.utils import get_concat_v
from DocuHive.data_science.doc_data_extraction.debug_image import show_image
from DocuHive.data_science.scripts.cv_extractor import CvDataExtractor

script_dir = os.path.dirname(os.path.abspath(__file__))
image_dir = f"{script_dir}/../data/cv"
image_files = [f"{script_dir}/../data/cv/cv3.pdf"] + [os.path.join(image_dir, f) for f in os.listdir(image_dir)]
testdata = image_files


@pytest.mark.parametrize("file_path", testdata)
def test_cv_extractor(file_path):
    if file_path.endswith("pdf"):
        page_dimensions = ""
        for page_number, page_image in enumerate(pdf2image.convert_from_path(file_path)):
            width, height = page_image.size
            if page_number == 0:
                page_dimensions += f"{width}x{height}"
                concatenated_image = page_image
            else:
                page_dimensions += f":{width}x{height}"
                concatenated_image = get_concat_v(concatenated_image, page_image)
        cv2_image = np.asarray(concatenated_image)
        fitz_doc = fitz.open(file_path)
    else:
        fitz_doc = None
        cv2_image = cv2.imread(file_path)
        page_dimensions = f"{cv2_image.shape[1]}x{cv2_image.shape[0]}"

    extract_cv_data = CvDataExtractor(job_id=0, doc_id=0)
    extract_cv_data.run(
        doc=fitz_doc,
        combined_image=cv2_image,
        page_dimensions=page_dimensions,
        debug=False,
        file_name=None,
    )
    # db_img = extract_cv_data.debug(["display_word_boxes", "display_page_split"])
    # show_image(db_img)
