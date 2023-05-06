import re
from DocuHive.data_science.doc_data_extraction.debug_image import (
    draw_polygons,
)
from DocuHive.data_science.doc_data_extraction.geometry import (
    get_rect_area,
    get_shared_area,
)


def get_words_in_region(words, left_y_limit):
    return [word for word in words if word["polygon"][2] > left_y_limit]


def get_fitz_words(doc, page_dimensions):
    words = []
    if doc is not None:
        page_1_words = doc[0].get_text("words")
        height_offset = 0
        if len(page_1_words) != 0:
            for i, page in enumerate(doc):
                img_w = int(page_dimensions.split(":")[i].split("x")[0])
                img_h = int(page_dimensions.split(":")[i].split("x")[1])
                pdf_w = page.mediabox_size.x
                pdf_h = page.mediabox_size.y
                width_scale = img_w / pdf_w
                height_scale = img_h / pdf_h
                page_words = page.get_text("words")
                for pos, w in enumerate(page_words):
                    x1 = int(w[0] * width_scale)
                    y1 = int(w[1] * height_scale) + height_offset
                    x2 = int(w[2] * width_scale)
                    y2 = int(w[3] * height_scale) + height_offset
                    page_words[pos] = (x1, y1, x2, y2) + w[4:]
                words += page_words
                height_offset += img_h + 10
    return words


def change_fitz_word_data_format(data):
    image_words = []
    for d in data:
        if d[4] not in [""]:
            x1, y1, x2, y2, text = d[:5]
            polygon = [x1, y1, x2, y2]
            poly_ints = list(map(int, polygon))
            polygon_string = ",".join(list(map(str, poly_ints)))
            image_words.append({"polygon": poly_ints, "text": text, "polygon_string": polygon_string})

    image_words = sorted(image_words, key=lambda x: x["polygon"][1])
    return image_words


def get_ocr_word_data(data, width_scale=1, height_scale=1):
    image_words = []
    for i in range(len(data["text"])):
        if (
            int(data["conf"][i]) > -1
            and data["text"][i].strip() != ""
            and data["width"][i] < 600
            and data["height"][i] < 150
        ):
            x1, y1, width, height, text = (
                data["left"][i],
                data["top"][i],
                data["width"][i],
                data["height"][i],
                data["text"][i],
            )
            x2 = x1 + width
            y2 = y1 + height
            polygon = [
                x1 * width_scale,
                y1 * height_scale,
                x2 * width_scale,
                y2 * height_scale,
            ]
            poly_ints = list(map(int, polygon))
            polygon_string = ",".join(list(map(str, poly_ints)))
            image_words.append({"polygon": poly_ints, "text": text, "polygon_string": polygon_string})
    return image_words


def extract_ocr_text(img, grey_image):
    import pytesseract
    if len(img) == 0:
        raise ValueError("Image file is empty")

    data_gray = pytesseract.image_to_data(grey_image, output_type="dict")
    # data_color = pytesseract.image_to_data(img, output_type="dict")

    # Handles empty pages
    if (
        len(data_gray["text"]) == 1
        and data_gray["height"][0] > 100
        # and len(data_color["text"]) == 1
        # and data_color["height"][0] > 100
    ):
        return []

    # Get the bounding boxes and text from the text regions
    data_gray = get_ocr_word_data(data_gray)
    # data_color = get_ocr_word_data(data_color)

    extra_words = []
    # for d_c in data_color:
    #     area = get_rect_area(d_c["polygon"])
    #     for d_g in data_gray:
    #         if get_shared_area(d_c["polygon"], d_g["polygon"]) / area > 0.2:
    #             break
    #     else:
    #         extra_words.append(d_c)

    image_words = data_gray + extra_words
    image_words = sorted(image_words, key=lambda x: x["polygon"][1])
    return image_words


def get_image_words(doc, page_dimensions, img, grey_image):
    image_words = get_fitz_words(doc, page_dimensions)
    if len(image_words) == 0:
        image_words = extract_ocr_text(img, grey_image)
    else:
        image_words = change_fitz_word_data_format(image_words)
    return image_words


def find_email(text):
    pattern = r"\b[A-Za-z0-9._]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b"
    match = re.search(pattern, text)
    if match:
        email = match.group()
        return email
    return None


def get_contact_info(image_sentences):
    for sentence in image_sentences:
        polygon = sentence["polygon"]
        text = sentence["text"]
        email = find_email(text)
        if email is not None:
            return {"text": email, "polygon": polygon}
    return {"text": None, "polygon": None}


def debug_words(img, image_words):
    return draw_polygons(img, [w["polygon"] for w in image_words])
