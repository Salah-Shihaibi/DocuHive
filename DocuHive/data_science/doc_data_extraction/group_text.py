import re
import cv2
from random import randint
from DocuHive.data_science.doc_data_extraction.debug_image import write_text_on_coord
from DocuHive.data_science.doc_data_extraction.geometry import (
    get_shared_area,
    get_rect_area,
    combine_rectangles,
    rect_dimensions,
)


def group_words_by_sentence(words, state=None, max_space=10, min_vertical_shared_pixels=5, change=True):
    group_polygons = {}
    for x, polygon in enumerate(words):
        poly = polygon["polygon"]
        curr_word_poly = polygon["polygon_string"]
        switch = True
        for range in list(group_polygons.keys()):
            r1x1, r1y1, r1x2, r1y2 = poly
            r2x1, r2y1, r2x2, r2y2 = map(int, range.split(","))

            if r1y2 < r2y1 or r1y1 > r2y2:
                continue

            if r2y2 >= r1y2:
                vert = r1y2 - r2y1
            else:
                vert = r2y2 - r1y1
            if vert <= min_vertical_shared_pixels:
                continue

            if (r1x1 - r2x2 <= max_space and r1x2 >= r2x2) or (r2x1 - r1x2 <= max_space and r2x2 >= r1x2):
                new_polygon = [
                    min(r1x1, r2x1),
                    min(r1y1, r2y1),
                    max(r1x2, r2x2),
                    max(r1y2, r2y2),
                ]
                new_polygon_ints = list(map(int, new_polygon))
                new_key = ",".join(list(map(str, new_polygon_ints)))

                del group_polygons[range]
                group_polygons.pop(curr_word_poly, None)
                group_polygons[new_key] = {
                    "polygon": new_polygon,
                    "polygon_string": new_key,
                    "text": None,
                }
                poly = new_polygon
                curr_word_poly = new_key
                switch = False
                change = True

        if switch:
            group_polygons[curr_word_poly] = {
                "polygon": poly,
                "polygon_string": curr_word_poly,
                "text": None,
            }

    if change:
        return group_words_by_sentence(group_polygons.values(), state, max_space, min_vertical_shared_pixels, False)
    else:
        return group_polygons


def get_blocks_using_sentences(sentences, min_horz_pecentage=50, vertical_distance=3, change=True):
    group_polygons = {}
    for sentence in sentences:
        poly = sentence["polygon"]
        curr_word_poly = sentence["polygon_string"]
        switch = True
        for range in list(group_polygons.keys()):
            r1x1, r1y1, r1x2, r1y2 = poly
            r2x1, r2y1, r2x2, r2y2 = map(int, range.split(","))

            if (
                abs(r1y1 - r2y2) > vertical_distance
                and abs(r1y2 - r2y1) > vertical_distance
                and get_shared_area((r1x1, r1y1, r1x2, r1y2), (r2x1, r2y1, r2x2, r2y2)) <= 0
            ):
                continue

            if r1x2 < r2x1 or r1x1 > r2x2:
                continue

            # if r2x2 >= r1x2:
            #     vert = r1x2 - r2x1
            # else:
            #     vert = r2x2 - r1x1
            #
            # shared_percentage_org = (vert / (r1x2 - r1x1))*100
            # shared_percentage = (vert / (r2x2 - r2x1)) * 100
            # if shared_percentage_org < min_horz_pecentage and shared_percentage < 90:
            #     continue

            new_polygon = [
                min(r1x1, r2x1),
                min(r1y1, r2y1),
                max(r1x2, r2x2),
                max(r1y2, r2y2),
            ]
            new_polygon_ints = list(map(int, new_polygon))
            new_key = ",".join(list(map(str, new_polygon_ints)))

            del group_polygons[range]
            group_polygons.pop(curr_word_poly, None)
            group_polygons[new_key] = {
                "polygon": new_polygon,
                "polygon_string": new_key,
                "text": None,
            }
            poly = new_polygon
            curr_word_poly = new_key
            switch = False
            change = True

        if switch:
            group_polygons[curr_word_poly] = {
                "polygon": poly,
                "polygon_string": curr_word_poly,
                "text": None,
            }

    if change:
        return get_blocks_using_sentences(group_polygons.values(), min_horz_pecentage, vertical_distance, False)
    else:
        return group_polygons


def get_text_in_sentences(image_sentences, image_words):
    for poly_string in image_sentences.keys():
        image_sentences[poly_string]["text"] = []
        sentence_box = image_sentences[poly_string]["polygon"]
        for word in image_words:
            word_box = word["polygon"]
            word_box_area = get_rect_area(word_box)
            if get_shared_area(sentence_box, word_box) / word_box_area > 0.8:
                image_sentences[poly_string]["text"].append(word)

        image_sentences[poly_string]["text"] = sorted(
            image_sentences[poly_string]["text"], key=lambda x: x["polygon"][0]
        )
        sentence_text = ""
        for s_words in image_sentences[poly_string]["text"]:
            sentence_text += f"{s_words['text']} "
        image_sentences[poly_string]["text"] = sentence_text
    return image_sentences


def get_text_in_block(polygon, image_sentences):
    sentences_combo = []
    for sentence in image_sentences:
        sentence_box = sentence["polygon"]
        sentence_box_area = get_rect_area(sentence_box)
        if get_shared_area(polygon, sentence_box) / sentence_box_area > 0.8:
            sentences_combo.append(sentence)

    sentences_combo = sorted(sentences_combo, key=lambda x: x["polygon"][1])
    if len(sentences_combo) == 0:
        return ""
    combo_height = sentences_combo[-1]["polygon"][3] - sentences_combo[-1]["polygon"][1] - 2

    collect_vertical_sentences = [[]]
    y_limit = sentences_combo[0]["polygon"][1]
    collect_vertical_sentences[0].append(sentences_combo[0])
    for s in sentences_combo[1:]:
        if abs(y_limit - s["polygon"][1]) < combo_height:
            collect_vertical_sentences[-1].append(s)
        else:
            y_limit = s["polygon"][1]
            collect_vertical_sentences[-1] = sorted(collect_vertical_sentences[-1], key=lambda x: x["polygon"][0])
            collect_vertical_sentences.append([s])
    collect_vertical_sentences[-1] = sorted(collect_vertical_sentences[-1], key=lambda x: x["polygon"][0])
    sentences_combo = [item for sublist in collect_vertical_sentences for item in sublist]

    text = sentences_combo[0]["text"]
    y_limit = sentences_combo[0]["polygon"][1]
    x2 = sentences_combo[0]["polygon"][2]
    for sentence_data in sentences_combo[1:]:
        gap = "\n"
        if abs(y_limit - sentence_data["polygon"][1]) < combo_height:
            gap_width = int((sentence_data["polygon"][0] - x2) / 20)
            gap = " " * gap_width
        text += f"{gap}{sentence_data['text']}"
        y_limit = sentence_data["polygon"][1]
        x2 = sentence_data["polygon"][2]
    return text


def split_sentences_by_gap(gap, image_sentences, img_height):
    if gap is not None:
        y_min = img_height * 0.4
        midpoint = gap[2] - 5

        image_sentences_copy = image_sentences.copy()
        for sentences_key, sentences_data in image_sentences_copy.items():
            x1, y1, x2, y2 = sentences_data["polygon"]
            if y1 > y_min and x1 < midpoint < x2 and (midpoint - x1) / midpoint > 0.7:
                p1 = [x1, y1, midpoint, y2]
                p1_key = ",".join(list(map(str, p1)))
                image_sentences[p1_key] = {
                    "polygon": p1,
                    "polygon_string": p1_key,
                    "text": None,
                }

                p2 = [gap[2], y1, x2, y2]
                p2_key = ",".join(list(map(str, p2)))
                image_sentences[p2_key] = {
                    "polygon": p2,
                    "polygon_string": p2_key,
                    "text": None,
                }

                del image_sentences[sentences_key]

    return image_sentences


def check_for_section(section, text, section_word, keywords_order):
    text = re.sub(r"^[^a-zA-Z]+|[^a-zA-Z]+$", "", text).lower()
    if keywords_order[section] == "start":
        if text.startswith(section_word.lower()):
            return True
    elif keywords_order[section] == "start end":
        if text.startswith(section_word.lower()) or text.endswith(section_word.lower()):
            return True
    else:
        return text.lower().find(section_word.lower()) != -1
    return False


def get_keyword_polygons(image_sentences, keywords, kw_polygons, keywords_order):
    kws = keywords.copy()
    for sentence in image_sentences:
        polygon = sentence["polygon"]
        text = sentence["text"]
        if len(text.strip()) <= 22:
            for section, wording in kws.items():
                switch = False
                for w in wording:
                    if check_for_section(section, text, w, keywords_order):
                        kw_polygons[section] = polygon
                        del kws[section]
                        switch = True
                        break
                if switch:
                    break
    return kw_polygons


def get_section_polygons(image_sections, kw_polygons, img_shape, gap):
    for section, kw_poly in kw_polygons.items():
        if kw_poly is not None:
            x1, y1, x2, y2 = kw_poly
            section_polygon = [10, y1, img_shape[1] - 10, img_shape[0]]
            if gap is not None:
                vertical_split = gap[2] - 5
                if x1 < vertical_split:
                    section_polygon[2] = vertical_split - 5
                elif x2 > vertical_split:
                    section_polygon[0] = vertical_split + 5
            image_sections[section]["polygon"] = section_polygon

    down_limits = [p for p in list(kw_polygons.values()) if p is not None]
    for section in image_sections.keys():
        if image_sections[section]["polygon"] is not None:
            for down_limit in down_limits:
                r1x1, r1y1, r1x2, r1y2 = image_sections[section]["polygon"]
                r2x1, r2y1, r2x2, r2y2 = down_limit
                if r1x2 < r2x1 or r1x1 > r2x2:
                    continue
                if r2y1 - r1y1 > 30 and r2y1 < r1y2:
                    image_sections[section]["polygon"][3] = r2y1 - 5
    return image_sections


def not_in_section(block, image_sections):
    for section_data in image_sections.values():
        if section_data["polygon"] is not None and get_shared_area(block, section_data["polygon"]) > 10:
            return False
    return True


def get_intro(image_blocks, image_words, image_sections, img_height):
    blocks_with_words = {key: 0 for key in image_blocks}
    for word in image_words:
        word_box = word["polygon"]
        word_box_area = get_rect_area(word_box)
        for block_poly in blocks_with_words.keys():
            block_poly_list = list(map(int, block_poly.split(",")))
            if get_shared_area(block_poly_list, word_box) / word_box_area > 0.8:
                blocks_with_words[block_poly] += 1
                break

    blocks_polys = list(image_blocks.values())
    blocks_polys = sorted(blocks_polys, key=lambda y: y["polygon"][1])
    for block in blocks_polys:
        poly = block["polygon"]
        x1, y1, x2, y2 = poly
        k = block["polygon_string"]
        if blocks_with_words[k] > 15 and y1 < img_height * 0.5 and not_in_section(poly, image_sections):
            return poly


def get_text_in_sections(image_sections, image_sentences):
    for section, section_data in image_sections.items():
        if section_data["polygon"] is not None:
            section_data["text"] = get_text_in_block(section_data["polygon"], image_sentences)
    return image_sections


def merge_blocks_on_page_splits(image_blocks, page_split_blocks):
    for image_block, image_blocks_info in image_blocks.items():
        for page_height in page_split_blocks.keys():
            lower = image_blocks_info["polygon"][1] - page_height
            upper = page_height - image_blocks_info["polygon"][3]
            if 0 < lower < 200:
                page_split_blocks[page_height]["lower"].append(image_blocks_info)
            if 0 < upper < 200:
                page_split_blocks[page_height]["upper"].append(image_blocks_info)
    for psb in page_split_blocks.values():
        for u in psb["upper"]:
            for l in psb["lower"]:
                if abs(u["polygon"][0] - l["polygon"][0]) < 10:
                    if u["polygon_string"] in image_blocks:
                        del image_blocks[u["polygon_string"]]
                    if l["polygon_string"] in image_blocks:
                        del image_blocks[l["polygon_string"]]
                    new_block = combine_rectangles(u["polygon"], l["polygon"])
                    new_block_str = ",".join(list(map(str, new_block)))
                    image_blocks[new_block_str] = {
                        "polygon": new_block,
                        "polygon_string": new_block_str,
                        "text": "",
                    }

    return image_blocks


def get_nearest_block(blocks_polys, direction, polygon):
    poly = None
    poly_diff = 10000
    for block in blocks_polys:
        block_poly = block["polygon"]
        if direction == "Nearst Bottom":
            diff = abs(block_poly[1] - polygon[3])
        elif direction == "Nearst Top":
            diff = abs(polygon[1] - block_poly[3])

        if (
            diff < poly_diff
            and (abs(block_poly[0] - polygon[0]) < 50 or abs(block_poly[2] - polygon[2]) < 50)
            and get_shared_area(block_poly, polygon) / get_rect_area(polygon) < 0.3
        ):
            poly = block_poly
            poly_diff = diff
    return poly


def get_blocks_between_limits(blocks_polys, image_sentences_list, bottom_limit, top_limit, width, height):
    text = ""
    extra_polygons = []
    for block in blocks_polys:
        block_poly = block["polygon"]
        if (
            block_poly[1] > top_limit
            and block_poly[3] < bottom_limit
            and rect_dimensions(block_poly)[0] > width
            and rect_dimensions(block_poly)[1] > height
        ):
            text += f"{get_text_in_block(block_poly, image_sentences_list)} \n\n"
            extra_polygons.append(block_poly)
    return extra_polygons, text


def debug_sentences(img, image_sentences):
    for sentence_data in image_sentences:
        x1, y1, x2, y2 = sentence_data["polygon"]
        c1 = (x1, y1)
        c2 = (x2, y2)
        rand_r, rand_g, rand_b = (
            randint(0, 255),
            randint(0, 255),
            randint(0, 255),
        )
        rgba_tuple = (rand_r, rand_g, rand_b)
        img = write_text_on_coord(img, sentence_data["polygon"], sentence_data["text"])
        cv2.rectangle(img, c1, c2, rgba_tuple, 2)
    return img


def debug_blocks(img, image_blocks):
    for block_data in image_blocks.values():
        x1, y1, x2, y2 = block_data["polygon"]
        c1 = (x1, y1)
        c2 = (x2, y2)
        rand_r, rand_g, rand_b = randint(0, 255), randint(0, 255), randint(0, 255)
        rgba_tuple = (rand_r, rand_g, rand_b)
        cv2.rectangle(img, c1, c2, rgba_tuple, 2)
    return img


def debug_keyword_polygons(img, kw_polygons, kw_colors):
    for section, section_data in kw_polygons.items():
        if section_data is not None:
            x1, y1, x2, y2 = section_data
            c1 = (x1, y1)
            c2 = (x2, y2)
            cv2.rectangle(img, c1, c2, kw_colors[section], 2)
    return img


def debug_intro_box(img, intro):
    if intro is not None:
        x1, y1, x2, y2 = intro["polygon"]
        c1 = (x1, y1)
        c2 = (x2, y2)
        cv2.rectangle(img, c1, c2, (0, 0, 255), 2)
    return img


def debug_section_text(img, image_sections, kw_colors):
    for section, section_data in image_sections.items():
        if section_data["polygon"] is not None:
            x1, y1, x2, y2 = section_data["polygon"]
            c1 = (x1, y1)
            c2 = (x2, y2)
            write_text_on_coord(
                img,
                section_data["polygon"],
                section_data["text"],
            )
            cv2.rectangle(img, c1, c2, kw_colors.get(section, (200, 200, 200)), 2)
    return img
