from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime
import os
import csv
import io
import statistics

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker


# ----------------------------------------------------------------------------
# Database setup
#
# The service stores ingested screener results in a simple SQL database. By
# default it uses a local SQLite file (screener.db) but you can override this
# by setting the DATABASE_URL environment variable to a SQLAlchemy‑compatible
# URL (for example, a PostgreSQL connection string on Neon or Supabase).
#
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./screener.db")
engine_kwargs = {}
# SQLite needs a special flag to allow concurrent connections on a single
# thread. Without this flag FastAPI will raise an error when the first
# request arrives.
if DATABASE_URL.startswith("sqlite"):  # pragma: no cover - runtime check
    engine_kwargs["connect_args"] = {"check_same_thread": False}

engine = create_engine(DATABASE_URL, **engine_kwargs)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ----------------------------------------------------------------------------
# Models
#
# The ScreenerResult model mirrors the JSON payload ingested by the service,
# while the Screener ORM model defines how results are stored in the database.
#
class ScreenerResult(BaseModel):
    ticker: str
    price: float
    suggested_strike: str = Field(..., alias="strike")
    call_bid: float
    premium_pct: float
    open_interest: int
    chain_url: str
    expiration: str
    earnings_before_expiration: bool = False
    exdiv_before_expiration: bool = False

    class Config:
        allow_population_by_field_name = True


class Screener(Base):
    __tablename__ = "screener_results"

    id = Column(Integer, primary_key=True, index=True)
    timestamp = Column(DateTime, default=datetime.utcnow, index=True)
    ticker = Column(String, index=True)
    price = Column(Float)
    suggested_strike = Column(String)
    call_bid = Column(Float)
    premium_pct = Column(Float)
    open_interest = Column(Integer)
    chain_url = Column(String)
    expiration = Column(String)
    earnings_before_expiration = Column(Boolean, default=False)
    exdiv_before_expiration = Column(Boolean, default=False)


# Create tables at startup
Base.metadata.create_all(bind=engine)


# ----------------------------------------------------------------------------
# FastAPI application
#
app = FastAPI(title="Covered Call Screener API")

# Configure CORS so that the frontend hosted on another domain can fetch data
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ----------------------------------------------------------------------------
# Utility functions
#
def get_db():
    """Yield a SQLAlchemy session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def latest_timestamp(db):
    """Return the most recent timestamp of any record in the table."""
    result = db.query(Screener.timestamp).order_by(Screener.timestamp.desc()).first()
    return result[0] if result else None


def filter_latest_entries(db):
    """Return all entries from the most recent ingestion batch.

    We identify the latest batch by looking at the maximum timestamp across
    records; all rows with that timestamp are considered part of the latest
    batch. We then exclude any rows where earnings_before_expiration or
    exdiv_before_expiration is true, since the user explicitly requested that
    names with earnings or ex‑div events before expiration be omitted.
    """
    ts = latest_timestamp(db)
    if not ts:
        return []
    return (
        db.query(Screener)
        .filter(Screener.timestamp == ts)
        .filter(Screener.earnings_before_expiration == False)
        .filter(Screener.exdiv_before_expiration == False)
        .all()
    )


def convert_to_dicts(records):
    """Convert a list of Screener ORM objects into dicts for JSON output."""
    return [
        {
            "ticker": r.ticker,
            "price": r.price,
            "suggested_strike": r.suggested_strike,
            "call_bid": r.call_bid,
            "premium_pct": r.premium_pct,
            "open_interest": r.open_interest,
            "chain_url": r.chain_url,
            "expiration": r.expiration,
        }
        for r in records
    ]


def generate_csv(records):
    """Generate a CSV string from Screener records."""
    output = io.StringIO()
    writer = csv.writer(output)
    # Header
    writer.writerow([
        "ticker",
        "price",
        "suggested_strike",
        "call_bid",
        "premium_pct",
        "open_interest",
        "chain_url",
        "expiration",
    ])
    # Rows
    for r in records:
        writer.writerow([
            r.ticker,
            r.price,
            r.suggested_strike,
            r.call_bid,
            r.premium_pct,
            r.open_interest,
            r.chain_url,
            r.expiration,
        ])
    return output.getvalue()


# ----------------------------------------------------------------------------
# API routes
#
@app.post("/ingest")
async def ingest(data: List[ScreenerResult], x_api_key: str = Header(None)):
    """Ingest a new batch of screener results.

    This endpoint expects a JSON array of objects matching the ScreenerResult
    schema. To prevent unauthorized ingestion, clients must include an
    `X‑API‑Key` header whose value matches the server's API_TOKEN environment
    variable. If the key is missing or invalid, a 401 error is returned.
    """
    api_token = os.environ.get("API_TOKEN", "changeme")
    if x_api_key != api_token:
        raise HTTPException(status_code=401, detail="Invalid API key")

    # Save each record in the database
    db = SessionLocal()
    try:
        timestamp = datetime.utcnow()
        for item in data:
            record = Screener(
                timestamp=timestamp,
                ticker=item.ticker,
                price=item.price,
                suggested_strike=item.suggested_strike,
                call_bid=item.call_bid,
                premium_pct=item.premium_pct,
                open_interest=item.open_interest,
                chain_url=item.chain_url,
                expiration=item.expiration,
                earnings_before_expiration=item.earnings_before_expiration,
                exdiv_before_expiration=item.exdiv_before_expiration,
            )
            db.add(record)
        db.commit()
    finally:
        db.close()
    return {"status": "success", "count": len(data)}


@app.get("/screener")
async def screener():
    """Return the latest batch of screener results as JSON.

    Only results without earnings/ex‑div events before expiration are returned.
    """
    db = SessionLocal()
    try:
        records = filter_latest_entries(db)
        return convert_to_dicts(records)
    finally:
        db.close()


@app.get("/latest.csv")
async def latest_csv():
    """Return the latest batch of screener results as a CSV file."""
    db = SessionLocal()
    try:
        records = filter_latest_entries(db)
        csv_data = generate_csv(records)
    finally:
        db.close()
    response = StreamingResponse(
        iter([csv_data]),
        media_type="text/csv",
    )
    response.headers["Content-Disposition"] = "attachment; filename=screener.csv"
    return response


@app.get("/latest.json")
async def latest_json():
    """Return the latest batch of screener results as a JSON array.

    This is simply an alias for the /screener endpoint but provided for
    convenience and clarity. Some clients prefer a file‑style endpoint.
    """
    return await screener()


@app.get("/stats")
async def stats():
    """Return basic statistics about the latest batch of results.

    The statistics include:
    - count: total number of records
    - median_open_interest: median of open interest values
    - average_premium_pct: average of premium_pct values (rounded to 4 decimals)
    If no data is available, zeroes are returned.
    """
    db = SessionLocal()
    try:
        records = filter_latest_entries(db)
        count = len(records)
        if count == 0:
            return {
                "count": 0,
                "median_open_interest": 0.0,
                "average_premium_pct": 0.0,
            }
        oi_values = [r.open_interest for r in records]
        premium_values = [r.premium_pct for r in records]
        median_oi = statistics.median(oi_values)
        avg_premium = statistics.mean(premium_values)
        return {
            "count": count,
            "median_open_interest": median_oi,
            "average_premium_pct": round(avg_premium, 4),
        }
    finally:
        db.close()