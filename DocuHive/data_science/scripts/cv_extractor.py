import pickle
import random
import cv2
import fitz
from typing import List, Optional
import numpy as np
from DocuHive.data_science.doc_data_extraction.feature_extractors import (
    get_vert_split_gaps,
    get_max_mid_page_gap,
    debug_vertical_gap,
)
from DocuHive.data_science.doc_data_extraction.group_text import (
    group_words_by_sentence,
    split_sentences_by_gap,
    get_text_in_sentences,
    get_blocks_using_sentences,
    get_keyword_polygons,
    get_section_polygons,
    get_intro,
    get_text_in_sections,
    debug_sentences,
    debug_blocks,
    debug_intro_box,
    debug_section_text,
)
from DocuHive.data_science.doc_data_extraction.semantic_search import (
    prep_semantic_search_vector,
)
from DocuHive.data_science.doc_data_extraction.text_extractors import (
    get_image_words,
    get_contact_info,
    debug_words,
)
from DocuHive.data_science.scripts.doc_extractor import (
    DocDataExtractor,
    TagData,
    transformed_data_for_storage,
    State,
)

# from DocuHive.tasks.tasks import prep_semantic_search_vector_task

keywords = {
    "Education": ["education", "academic history"],
    "Experience": [
        "experience",
        "employment",
        "work history",
        "employment history" "career",
        "work",
    ],
    "Skills": [
        "key skills",
        "skills",
        "highlights",
        "areas of expertise",
        "core qualifications",
    ],
    "Contact": ["contact"],
    "Hobbies": ["hobbies", "interests", "passions"],
    "Certificates": ["courses", "certificates", "extracurriculars", "certifications"],
    "Achievements": ["accomplishments", "achievements"],
    "Summary": ["summary", "profile", "statement"],
    "Languages": ["language"],
    "Reference": ["reference"],
    "Activities": ["projects", "publications", "activities", "memberships"],
}

keywords_order = {
    "Education": "start",
    "Experience": "start end",
    "Skills": "start end",
    "Contact": "start end",
    "Hobbies": "start end",
    "Certificates": "start end",
    "Achievements": "start end",
    "Summary": "start end",
    "Languages": "start",
    "Reference": "start",
    "Activities": "start end",
}

kw_colors = {
    key: (
        random.randint(0, 255),
        random.randint(0, 255),
        random.randint(0, 255),
    )
    for key in keywords
}

semantic_search_data = [
    "Summary",
    "Experience",
    "Education",
    "Certificates",
    "Languages",
    "Skills",
    "Hobbies",
    "Achievements",
    "Activities",
]


class CvDataExtractor(DocDataExtractor):
    def run(
        self,
        doc: fitz.Document,
        combined_image: np.ndarray,
        file_name: str,
        page_dimensions: str,
        debug: bool,
    ) -> Optional[List[TagData]]:
        try:
            img = cv2.cvtColor(combined_image, cv2.COLOR_RGB2BGR)
        except cv2.error:
            img = combined_image
        grey_image = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        img_height, image_width, _ = img.shape

        image_words = get_image_words(doc=doc, img=img, grey_image=grey_image, page_dimensions=page_dimensions)
        image_sentences = group_words_by_sentence(image_words, max_space=20)

        gaps = get_vert_split_gaps(grey_image=grey_image, min_height_percent=0.6, img_height=img_height)
        max_gap = get_max_mid_page_gap(gaps=gaps, img_width=image_width, width=5)
        image_sentences = split_sentences_by_gap(gap=max_gap, image_sentences=image_sentences, img_height=img_height)
        image_sentences = get_text_in_sentences(image_sentences=image_sentences, image_words=image_words)

        image_sentences_list = list(image_sentences.values())
        image_blocks = get_blocks_using_sentences(
            sentences=image_sentences_list, min_horz_pecentage=30, vertical_distance=20
        )

        kw_polygons = {key: None for key in keywords}
        image_sections = {key: {"polygon": None, "text": None} for key in keywords}

        kw_polygons = get_keyword_polygons(
            image_sentences=image_sentences_list,
            keywords=keywords,
            kw_polygons=kw_polygons,
            keywords_order=keywords_order,
        )
        image_sections = get_section_polygons(
            image_sections=image_sections,
            kw_polygons=kw_polygons,
            img_shape=img.shape,
            gap=max_gap,
        )

        if image_sections["Summary"]["polygon"] is None:
            image_sections["Summary"]["polygon"] = get_intro(
                image_blocks=image_blocks,
                image_words=image_words,
                image_sections=image_sections,
                img_height=img_height,
            )

        image_sections = get_text_in_sections(image_sections=image_sections, image_sentences=image_sentences_list)
        image_sections["Email"] = get_contact_info(image_sentences=image_sentences_list)

        if debug:
            self.debug_data = State()
            self.debug_data.img = img
            self.debug_data.image_words = image_words
            self.debug_data.image_sentences = image_sentences_list
            self.debug_data.page_split = max_gap
            self.debug_data.image_blocks = image_blocks
            self.debug_data.image_sections = image_sections
            return

        semantic_search_text = "\n".join(
            [image_sections[sect]["text"] for sect in semantic_search_data if image_sections[sect]["text"]]
        )
        image_sections["Semantic Search Blob"] = {"text": prep_semantic_search_vector(semantic_search_text)}
        # prep_semantic_search_vector_task.delay(semantic_search_text, file_name, self.doc_id, self.job_id, "cv")
        return transformed_data_for_storage(image_sections=image_sections)

    def debug(self, debug_tags: list) -> np.ndarray:
        debug_img = self.debug_data.img

        if "display_word_boxes" in debug_tags:
            debug_img = debug_words(img=debug_img, image_words=self.debug_data.image_words)
        if "display_page_split" in debug_tags:
            debug_img = debug_vertical_gap(img=debug_img, gap=self.debug_data.page_split)
        if "display_sentences" in debug_tags:
            debug_img = debug_sentences(img=debug_img, image_sentences=self.debug_data.image_sentences)
        if "display_blocks" in debug_tags:
            debug_img = debug_blocks(img=debug_img, image_blocks=self.debug_data.image_blocks)
        if "display_summary" in debug_tags:
            debug_img = debug_intro_box(img=debug_img, intro=self.debug_data.image_sections["Summary"])
        if "display_sections" in debug_tags:
            debug_img = debug_section_text(
                img=debug_img,
                image_sections=self.debug_data.image_sections,
                kw_colors=kw_colors,
            )
        return debug_img
