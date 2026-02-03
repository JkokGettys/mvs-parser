import os
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, DateTime, Index, Text, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy.sql import func

Base = declarative_base()

# Lazy initialization - don't connect at import time
_engine = None
_SessionLocal = None


def get_engine():
    global _engine
    if _engine is None:
        database_url = os.getenv('MVS_DATABASE_URL') or os.getenv('DATABASE_PUBLIC_URL') or os.getenv('DATABASE_URL')
        if not database_url:
            raise ValueError("No database URL found. Set MVS_DATABASE_URL environment variable.")
        
        connect_args = {}
        if 'railway' in database_url:
            connect_args = {'sslmode': 'require'}
        _engine = create_engine(database_url, connect_args=connect_args)
    return _engine


def get_session_local():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
    return _SessionLocal


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


class BaseCostTable(Base):
    """Metadata for each base cost table from MVS"""
    __tablename__ = 'mvs_base_cost_tables'
    
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    occupancy_code = Column(String(20), nullable=True)
    section = Column(Integer, nullable=False)
    page = Column(Integer, nullable=False)
    pdf_page = Column(Integer, nullable=True)
    notes = Column(Text, nullable=True)
    file_name = Column(String(255), nullable=True)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationship to cost rows
    rows = relationship("BaseCostRow", back_populates="table", cascade="all, delete-orphan")


class BaseCostRow(Base):
    """Individual cost rows within a base cost table"""
    __tablename__ = 'mvs_base_cost_rows'
    
    id = Column(Integer, primary_key=True)
    table_id = Column(Integer, ForeignKey('mvs_base_cost_tables.id', ondelete='CASCADE'), nullable=False)
    
    # Core cost data columns
    building_class = Column(Text, nullable=False)
    quality_type = Column(Text, nullable=True)
    exterior_walls = Column(Text, nullable=True)
    interior_finish = Column(Text, nullable=True)
    lighting_plumbing = Column(Text, nullable=True)
    heat = Column(String(255), nullable=True)
    
    # Cost values
    cost_sqm = Column(Numeric(10, 2), nullable=True)
    cost_cuft = Column(Numeric(10, 2), nullable=True)
    cost_sqft = Column(Numeric(10, 2), nullable=True)
    
    # Row ordering
    row_order = Column(Integer, default=0)
    
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationship back to table
    table = relationship("BaseCostTable", back_populates="rows")


class ElevatorType(Base):
    """Elevator type definitions from MVS Section 58"""
    __tablename__ = 'mvs_elevator_types'
    
    id = Column(Integer, primary_key=True)
    category = Column(String(50), nullable=False)  # 'passenger' or 'freight'
    name = Column(String(255), nullable=False)
    source_page = Column(Integer, default=701)
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationships
    costs = relationship("ElevatorCost", back_populates="elevator_type", cascade="all, delete-orphan")
    cost_per_stops = relationship("ElevatorCostPerStop", back_populates="elevator_type", cascade="all, delete-orphan")


class ElevatorCost(Base):
    """Individual elevator cost entries (speed x capacity matrix)"""
    __tablename__ = 'mvs_elevator_costs'
    
    id = Column(Integer, primary_key=True)
    elevator_type_id = Column(Integer, ForeignKey('mvs_elevator_types.id', ondelete='CASCADE'), nullable=False)
    speed_fpm = Column(Integer, nullable=False)  # feet per minute
    capacity_lbs = Column(Integer, nullable=False)  # pounds
    base_cost = Column(Numeric(12, 2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    elevator_type = relationship("ElevatorType", back_populates="costs")


class ElevatorCostPerStop(Base):
    """Cost per additional stop for elevators"""
    __tablename__ = 'mvs_elevator_cost_per_stop'
    
    id = Column(Integer, primary_key=True)
    elevator_type_id = Column(Integer, ForeignKey('mvs_elevator_types.id', ondelete='CASCADE'), nullable=False)
    capacity_lbs = Column(Integer, nullable=False)
    door_type = Column(String(20), nullable=False)  # 'standard', 'manual', 'power'
    cost_per_stop = Column(Numeric(10, 2), nullable=False)
    created_at = Column(DateTime, server_default=func.now())
    
    elevator_type = relationship("ElevatorType", back_populates="cost_per_stops")


def get_db():
    SessionLocal = get_session_local()
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    Base.metadata.create_all(bind=get_engine())
