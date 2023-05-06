from DocuHive.database.models import TagDB, DataType, LabelDB, JobDB, WorkflowDB
from DocuHive.database.setup import db

cv = [
    {"tag_name": "Education", "tag_type": DataType.text},
    {"tag_name": "Experience", "tag_type": DataType.text},
    {"tag_name": "Skills", "tag_type": DataType.text},
    {"tag_name": "Contact", "tag_type": DataType.text},
    {"tag_name": "Hobbies", "tag_type": DataType.text},
    {"tag_name": "Certificates", "tag_type": DataType.text},
    {"tag_name": "Achievements", "tag_type": DataType.text},
    {"tag_name": "Summary", "tag_type": DataType.text},
    {"tag_name": "Languages", "tag_type": DataType.text},
    {"tag_name": "Reference", "tag_type": DataType.text},
    {"tag_name": "Email", "tag_type": DataType.text},
    {"tag_name": "Activities", "tag_type": DataType.text},
    {"tag_name": "Semantic Search Blob", "tag_type": DataType.blob},
]


movies = [
    {"tag_name": "Title", "tag_type": DataType.text},
    {"tag_name": "TOMATOMETER", "tag_type": DataType.integer},
    {"tag_name": "Audience Score", "tag_type": DataType.integer},
    {"tag_name": "Movie Info", "tag_type": DataType.text},
    {"tag_name": "Genre", "tag_type": DataType.text},
    {"tag_name": "Original Language", "tag_type": DataType.text},
    {"tag_name": "Director", "tag_type": DataType.text},
    {"tag_name": "Producer", "tag_type": DataType.text},
    {"tag_name": "Release Date (Theaters)", "tag_type": DataType.text},
    {"tag_name": "Runtime", "tag_type": DataType.text},
    {"tag_name": "Distributor", "tag_type": DataType.text},
    {"tag_name": "Production Co", "tag_type": DataType.text},
    {"tag_name": "Cast & Crew", "tag_type": DataType.text},
    {"tag_name": "Critic Reviews", "tag_type": DataType.text},
]

general_tags = [
    {"tag_name": "page", "tag_type": DataType.integer},
]

tags = general_tags + cv + movies

labels = {"cv": cv, "movie": movies}

cv_extractor = {
    "label": "cv",
    "debug_options": [
        "display_word_boxes",
        "display_page_split",
        "display_sentences",
        "display_blocks",
        "display_summary",
        "display_sections",
    ],
}

movie_extractor = {
    "label": "movie",
    "debug_options": [
        "display_word_boxes",
        "display_sentences",
        "display_blocks",
        "display_page_split_blocks",
        "display_sections",
    ],
}

workflows = {
    "generic_cv_extractor": cv_extractor,
    "generic_cv_extractor2": cv_extractor,
    "movie_extractor": movie_extractor,
    "movie_extractor2": movie_extractor,
}


def seed_tags():
    for tag in tags:
        tag_db = TagDB.query.filter(TagDB.name == tag["tag_name"]).first()
        if not tag_db:
            tag_db = TagDB(name=tag["tag_name"], data_type=tag["tag_type"])
            db.session.add(tag_db)
    db.session.commit()


def seed_tags_labels_and_workflows():
    seed_tags()
    for label, label_tags in labels.items():
        label_db = LabelDB.query.filter(LabelDB.name == label).first()
        if not label_db:
            label_db = LabelDB(name=label)
            for tag in label_tags:
                tag = TagDB.query.filter(TagDB.name == tag["tag_name"]).first()
                label_db.tags.append(tag)
            db.session.add(label_db)

    for workflow_name, workflow_data in workflows.items():
        job_db = WorkflowDB.query.filter(WorkflowDB.name == workflow_name).first()
        if not job_db:
            label_db = LabelDB.query.filter(LabelDB.name == workflow_data["label"]).first()
            debug_options = "$:$".join(workflow_data["debug_options"])
            wf_db = WorkflowDB(name=workflow_name, label=label_db, debug_options=debug_options)
            db.session.add(wf_db)

    db.session.commit()
