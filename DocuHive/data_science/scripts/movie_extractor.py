import pickle

import cv2
import fitz
from typing import List, Optional
import numpy as np

from DocuHive.data_science.doc_data_extraction.debug_image import write_text_on_coord
from DocuHive.data_science.doc_data_extraction.geometry import get_coords
from DocuHive.data_science.doc_data_extraction.group_text import (
    group_words_by_sentence,
    get_text_in_sentences,
    get_blocks_using_sentences,
    debug_sentences,
    debug_blocks,
    get_text_in_block,
    merge_blocks_on_page_splits,
    get_nearest_block,
    get_blocks_between_limits,
)
from DocuHive.data_science.doc_data_extraction.semantic_search import prep_semantic_search_vector
from DocuHive.data_science.doc_data_extraction.text_extractors import (
    get_image_words,
    debug_words,
    get_words_in_region,
)
from DocuHive.data_science.scripts.doc_extractor import (
    DocDataExtractor,
    TagData,
    State,
    transformed_data_for_storage,
)


find_kws = {
    "Title": {"polygon": None, "algo": "Nearst Top", "text": None},
    "TOMATOMETER": {"polygon": None, "algo": "Nearst Top", "text": None},
    "Audience Score": {"polygon": None, "algo": "Nearst Top", "text": None},
    "Movie Info": {"polygon": None, "algo": "Nearst Bottom", "text": None},
    "Genre": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Original Language": {
        "polygon": None,
        "algo": "Key Valur Pair",
        "text": None,
    },
    "Director": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Producer": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Release Date (Theaters)": {
        "polygon": None,
        "algo": "Key Valur Pair",
        "text": None,
    },
    "Runtime": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Distributor": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Production Co": {"polygon": None, "algo": "Key Valur Pair", "text": None},
    "Cast & Crew": {
        "polygon": None,
        "algo": "Bottom",
        "limit": "Hide Cast & Crew",
        "text": None,
    },
    "Hide Cast & Crew": {"polygon": None, "algo": None},
    "Critic Reviews": {
        "polygon": None,
        "algo": "Bottom",
        "limit": "View All Critic Reviews",
        "text": None,
    },
    "View All Critic Reviews": {"polygon": None, "algo": None},
}


semantic_search_data = [
    "Movie Info",
    "Genre",
    "Critic Reviews",
    "Original Language",
    "Cast & Crew",
]


class MovieDataExtractor(DocDataExtractor):
    def run(
        self,
        doc: fitz.Document,
        combined_image: np.ndarray,
        file_name: str,
        page_dimensions: str,
        debug: bool,
    ) -> Optional[List[TagData]]:
        global find_kws
        find_kw = find_kws.copy()
        try:
            img = cv2.cvtColor(combined_image, cv2.COLOR_RGB2BGR)
        except cv2.error:
            img = combined_image
        grey_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        img_height, image_width, _ = img.shape
        page_heights = [int(dim.split("x")[1]) for dim in page_dimensions.split(":")]
        page_split_blocks = {sum(page_heights[: i + 1]): {"upper": [], "lower": []} for i in range(len(page_heights))}

        image_words = get_image_words(doc=doc, img=img, grey_image=grey_image, page_dimensions=page_dimensions)
        image_words = get_words_in_region(image_words, left_y_limit=750)
        image_sentences = group_words_by_sentence(image_words, max_space=20)
        image_sentences = get_text_in_sentences(image_sentences=image_sentences, image_words=image_words)

        for polygon_str, sentences_info in image_sentences.copy().items():
            if sentences_info["text"].strip().endswith("- Rotten Tomatoes"):
                if find_kw["Title"]["text"] is None:
                    find_kw["Title"]["text"] = sentences_info["text"].strip()
                    find_kw["Title"]["polygon"] = sentences_info["polygon"]
                del image_sentences[polygon_str]

        image_sentences_list = list(image_sentences.values())
        image_blocks = get_blocks_using_sentences(
            sentences=image_sentences_list, min_horz_pecentage=30, vertical_distance=15
        )

        image_blocks = merge_blocks_on_page_splits(image_blocks, page_split_blocks)
        blocks_polys = list(image_blocks.values())
        blocks_polys = sorted(blocks_polys, key=lambda y: y["polygon"][1])

        for sentence in image_sentences_list:
            polygon = sentence["polygon"]
            text = sentence["text"].strip().lower()
            for kw, info in find_kw.items():
                kw = kw.lower()
                if info["algo"] == "Key Valur Pair":
                    if text.startswith(f"{kw}:"):
                        info["polygon"] = polygon
                        info["text"] = text.split(":")[1]
                elif info["algo"] in ["Nearst Bottom", "Nearst Top"]:
                    if text == kw or text.startswith(f"{kw} for "):
                        poly = get_nearest_block(blocks_polys, info["algo"], polygon)
                        info["polygon"] = poly
                        block_text = get_text_in_block(poly, image_sentences_list).strip()
                        info["text"] = block_text.rstrip("%") if block_text.endswith("%") else block_text

                elif info["algo"] == "Bottom":
                    if text == kw or text.startswith(f"{kw} for "):
                        info["polygon"] = polygon
                else:
                    if text.startswith(kw):
                        info["polygon"] = polygon

        crew = find_kw["Cast & Crew"]
        extra_polygons, text = get_blocks_between_limits(
            blocks_polys,
            image_sentences_list,
            bottom_limit=find_kw[crew["limit"]]["polygon"][1],
            top_limit=crew["polygon"][3],
            width=40,
            height=30,
        )
        crew["text"] = text
        crew["extra_polygons"] = extra_polygons
        del find_kw[crew["limit"]]

        reviews = find_kw["Critic Reviews"]
        extra_polygons, text = get_blocks_between_limits(
            blocks_polys,
            image_sentences_list,
            bottom_limit=find_kw[reviews["limit"]]["polygon"][1],
            top_limit=reviews["polygon"][3],
            width=320,
            height=35,
        )
        reviews["text"] = text
        reviews["extra_polygons"] = extra_polygons
        del find_kw[reviews["limit"]]

        if debug:
            self.debug_data = State()
            self.debug_data.img = img
            self.debug_data.image_words = image_words
            self.debug_data.image_sentences = image_sentences_list
            self.debug_data.page_split_blocks = page_split_blocks
            self.debug_data.image_blocks = image_blocks
            self.debug_data.sections = find_kw
            return

        semantic_search_text = "\n".join(
            [find_kw[sect]["text"] for sect in semantic_search_data if find_kw[sect]["text"]]
        )
        find_kw["Semantic Search Blob"] = {"text": prep_semantic_search_vector(semantic_search_text)}
        return transformed_data_for_storage(image_sections=find_kw)

    def debug(self, debug_tags: list) -> np.ndarray:
        debug_img = self.debug_data.img
        if "display_word_boxes" in debug_tags:
            debug_img = debug_words(img=debug_img, image_words=self.debug_data.image_words)
        if "display_sentences" in debug_tags:
            debug_img = debug_sentences(img=debug_img, image_sentences=self.debug_data.image_sentences)
        if "display_blocks" in debug_tags:
            debug_img = debug_blocks(img=debug_img, image_blocks=self.debug_data.image_blocks)
        if "display_page_split_blocks" in debug_tags:
            for blocks_info in self.debug_data.page_split_blocks.values():
                for polys in blocks_info["upper"]:
                    c1, c2, _ = get_coords(polys["polygon"])
                    cv2.rectangle(debug_img, c1, c2, (255, 255, 0), 2)
                for polys in blocks_info["lower"]:
                    c1, c2, _ = get_coords(polys["polygon"])
                    cv2.rectangle(debug_img, c1, c2, (255, 0, 255), 2)

        if "display_sections" in debug_tags:
            for kw, info in self.debug_data.sections.items():
                if info.get("polygon") is not None:
                    c1, c2, _ = get_coords(info["polygon"])
                    debug_img = write_text_on_coord(debug_img, info["polygon"], kw)
                    cv2.rectangle(debug_img, c1, c2, (0, 255, 255), 2)
                for poly in info.get("extra_polygons", []):
                    c1, c2, _ = get_coords(poly)
                    debug_img = write_text_on_coord(debug_img, poly, kw)
                    cv2.rectangle(debug_img, c1, c2, (0, 255, 255), 2)

        return debug_img
