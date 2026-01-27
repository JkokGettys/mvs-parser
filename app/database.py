from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, DateTime, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import func
from app.config import DATABASE_URL

# Handle Railway SSL
connect_args = {}
if 'railway' in DATABASE_URL:
    connect_args = {'sslmode': 'require'}

engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


class LocalMultiplier(Base):
    __tablename__ = 'mvs_local_multipliers'
    
    id = Column(Integer, primary_key=True)
    location = Column(String(255), nullable=False)
    city = Column(String(255), nullable=True)
    region = Column(String(255), nullable=False)
    country = Column(String(100), nullable=False)
    class_a = Column(Numeric(5, 3), nullable=False)
    class_b = Column(Numeric(5, 3), nullable=False)
    class_c = Column(Numeric(5, 3), nullable=False)
    class_d = Column(Numeric(5, 3), nullable=False)
    class_s = Column(Numeric(5, 3), nullable=False)
    source_page = Column(Integer, nullable=False)
    is_regional = Column(Boolean, default=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class CurrentCostMultiplier(Base):
    __tablename__ = 'mvs_current_cost_multipliers'
    
    id = Column(Integer, primary_key=True)
    method = Column(String(50), nullable=False)
    region = Column(String(50), nullable=False)
    building_class = Column(String(10), nullable=False)
    effective_date = Column(String(20), nullable=False)
    multiplier = Column(Numeric(5, 3), nullable=False)
    source_page = Column(Integer, nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class StoryHeightMultiplier(Base):
    __tablename__ = 'mvs_story_height_multipliers'
    
    id = Column(Integer, primary_key=True)
    height_meters = Column(Numeric(6, 2), nullable=False)
    height_feet = Column(Integer, nullable=False)
    sqft_multiplier = Column(Numeric(6, 4), nullable=False)
    cuft_multiplier = Column(Numeric(6, 4), nullable=False)
    source_page = Column(Integer, default=90)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class FloorAreaPerimeterMultiplier(Base):
    __tablename__ = 'mvs_floor_area_perimeter_multipliers'
    
    id = Column(Integer, primary_key=True)
    floor_area_sqft = Column(Integer, nullable=False)
    perimeter_ft = Column(Integer, nullable=False)
    multiplier = Column(Numeric(6, 4), nullable=False)
    source_page = Column(Integer, default=90)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class RegionMapping(Base):
    __tablename__ = 'mvs_region_mappings'
    
    id = Column(Integer, primary_key=True)
    state_code = Column(String(5), nullable=False, unique=True)
    state_name = Column(String(100), nullable=False)
    current_cost_region = Column(String(50), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=engine)
