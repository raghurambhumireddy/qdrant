from fastapi import FastAPI, HTTPException, Body
from pydantic import BaseModel
from qdrant_client import QdrantClient
from qdrant_client import models
import re

app = FastAPI()

# Connect to Qdrant
qdrant_client = QdrantClient(
    url="https://c8e964b9-aa62-4a53-87dd-7252467c2b02.europe-west3-0.gcp.cloud.qdrant.io:6333",
    api_key="bVrqBpHHscZySyi4qKnTWDpm4JdmMXMnFbhLCD2dxvJKZbrmUQ11pA"
)

collect_name = "testing_7_master_sample_data"

class FetchRequest(BaseModel):
    doc_id: int

def clean_value(value):
    value = re.sub(' +', ' ', value)
    return value.strip()

def clean_record(record):
    if 'paragraph' in record:
        record['paragraph'] = clean_value(record['paragraph'])
    if 'phrase' in record:
        record['phrase'] = clean_value(record['phrase'])
    return record

def replace_special_characters(record):
    for key, value in record.items():
        if isinstance(value, str):
            value = value.replace('\n', ' ').replace('\t', ' ')
            record[key] = value
    return record

def id_call(id, limit):
    return qdrant_client.scroll(
        collection_name=collect_name,
        scroll_filter=models.Filter(
            must=[
                models.FieldCondition(
                    key="doc",
                    match=models.MatchValue(value=id),
                ),
            ]
        ),
        limit=limit
    )

@app.post("/fetch_doc")
def fetch_document(request: FetchRequest):
    doc_id = request.doc_id

    # Query Qdrant to fetch document
    try:
        out = id_call(id=int(doc_id), limit=1000)
        if out:
            records = out[0]
            data = []

            for record in records:
                row = {'id': record.id}
                row.update(record.payload)
                row = replace_special_characters(row)
                row = clean_record(row)
                data.append(row)

            data.sort(key=lambda x: x.get('version', 0), reverse=True)
            return {"data": data}  # Returning the data in JSON format
        else:
            raise HTTPException(status_code=404, detail="Document not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
