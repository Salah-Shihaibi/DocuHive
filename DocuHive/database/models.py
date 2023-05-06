import enum

from sqlalchemy import (
    Integer,
    String,
    Float,
    Boolean,
    DATE,
    Column,
    Enum,
    Table,
    ForeignKey,
    DateTime,
    func,
    LargeBinary,
)
from sqlalchemy.orm import relationship
from DocuHive.database.setup import db

Model = db.Model


class DataType(enum.Enum):
    file = 1
    collection = 2
    text = 3
    integer = 4
    float = 5
    boolean = 6
    date = 7
    blob = 8


class LabelTagDB(Model):
    __tablename__ = "label_tag_db"
    tag_id = Column(Integer, ForeignKey("tag_db.id"), primary_key=True)
    label_id = Column(Integer, ForeignKey("label_db.id"), primary_key=True)


class LabelDB(Model):
    __tablename__ = "label_db"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    tags = relationship("TagDB", secondary="label_tag_db", back_populates="labels")
    workflows = relationship("WorkflowDB", back_populates="label")


class TagDB(Model):
    __tablename__ = "tag_db"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    data_type = Column(Enum(DataType), nullable=True)
    labels = relationship("LabelDB", secondary="label_tag_db", back_populates="tags")


class WorkflowDB(Model):
    __tablename__ = "workflow_db"
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    debug_options = Column(String)
    label_id = Column(Integer, ForeignKey("label_db.id"))
    label = relationship("LabelDB", back_populates="workflows")
    jobs = relationship("JobDB", back_populates="workflow")


data_job_table = Table(
    "data_job_table",
    Model.metadata,
    Column("data_id", ForeignKey("data_db.id")),
    Column("job_id", ForeignKey("job_db.id")),
)


job_data_table = Table(
    "job_data_table",
    Model.metadata,
    Column("job_id", ForeignKey("job_db.id")),
    Column("data_id", ForeignKey("data_db.id")),
)


class JobDB(Model):
    __tablename__ = "job_db"
    id = Column(Integer, primary_key=True)
    identifier = Column(String, unique=True)
    task_id = Column(String)
    arguments = Column(String)
    status = Column(String)
    status_message = Column(String)
    time_created = Column(DateTime(timezone=True), server_default=func.now())
    time_updated = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.current_timestamp(),
    )
    datas = relationship("DataDB", secondary=job_data_table, cascade="all, delete")
    datas_reversed = relationship("DataDB", secondary=data_job_table, viewonly=True)

    workflow_id = Column(Integer, ForeignKey("workflow_db.id"))
    workflow = relationship("WorkflowDB", back_populates="jobs")
    # parent_id = Column(Integer, ForeignKey('job_db.id'))
    # parent = relationship("JobDB", remote_side=[id], back_populates="children")
    # children = relationship("JobDB", back_populates="parent")


class DataDB(Model):
    __tablename__ = "data_db"
    id = Column(Integer, primary_key=True)
    data_type = Column(Enum(DataType))
    name = Column(String, unique=True, nullable=True)
    page_dimensions = Column(String, nullable=True)
    polygons = Column(String, nullable=True)
    float = Column(Float, nullable=True)
    integer = Column(Integer, nullable=True)
    boolean = Column(Boolean, nullable=True)
    text = Column(String, nullable=True)
    date = Column(DATE, nullable=True)
    blob = Column(LargeBinary, nullable=True)
    pdf_blob = Column(String, nullable=True)
    image_blob = Column(String, nullable=True)
    jobs = relationship("JobDB", secondary=data_job_table, cascade="all, delete")
    jobs_reverse = relationship("JobDB", secondary=job_data_table, viewonly=True)

    data_tag_data = relationship(
        "DataTagDataDB",
        back_populates="data",
        foreign_keys="DataTagDataDB.data_id",
        cascade="all, delete",
    )
    data_tag_parent_file = relationship(
        "DataTagDataDB",
        back_populates="parent_file_data",
        foreign_keys="DataTagDataDB.parent_file_id",
    )
    data_tag_parent_collection = relationship(
        "DataTagDataDB",
        back_populates="parent_collection_data",
        foreign_keys="DataTagDataDB.parent_collection_id",
    )

    # parent_id = Column(Integer, ForeignKey("data_db.id"))
    # parent = relationship("DataDB", remote_side=[id], back_populates="children")
    # children = relationship("DataDB", back_populates="parent")


class DataTagDataDB(Model):
    __tablename__ = "data_tag_data_db"
    parent_file_id = Column(ForeignKey("data_db.id"), primary_key=True)
    parent_file_data = relationship("DataDB", foreign_keys=[parent_file_id])

    data_id = Column(ForeignKey("data_db.id"), primary_key=True)
    data = relationship("DataDB", foreign_keys=[data_id])

    tag_id = Column(ForeignKey("tag_db.id"), primary_key=True)
    tag = relationship("TagDB", foreign_keys=[tag_id])

    parent_collection_id = Column(ForeignKey("data_db.id"))
    parent_collection_data = relationship("DataDB", foreign_keys=[parent_collection_id])

    label_id = Column(ForeignKey("label_db.id"))
    label = relationship("LabelDB", foreign_keys=[label_id])

    job_name = Column(String)
    # workflow_id = Column(ForeignKey("job_id.id"))
    # workflow = relationship("JobDB", foreign_keys=[workflow_id])
    # selected_tag = Column(Boolean)
