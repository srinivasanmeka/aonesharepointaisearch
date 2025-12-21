import json
import base64
import os
import boto3
import psycopg
from pgvector.psycopg import register_vector
from pypdf import PdfReader
import io
import uuid
from datetime import datetime, timezone
from langchain_text_splitters import RecursiveCharacterTextSplitter


AWS_REGION = "us-east-1"
BEDROCK_EMBEDDING_MODEL = "cohere.embed-english-v3"
SECRET_NAME = "rds-db-credentials/cluster-CZRGZG2DXJGCQGM4OZTEHRR2SU/postgres/1762906251764"


# Get PostgreSQL connection - pgvector enabled

def get_db_connection():

    secmgr = boto3.client("secretsmanager", region_name=AWS_REGION)
    secret_dict = json.loads(secmgr.get_secret_value(SecretId=SECRET_NAME)["SecretString"])

    conn = psycopg.connect(
        host=secret_dict["host"],
        dbname="aonebankpgvdb",
        user=secret_dict["username"],
        password=secret_dict["password"],
        port=secret_dict["port"]
    )
    register_vector(conn)
    return conn



# Extract PDF text from local aws tmp file

def extract_text_from_pdf(tmp_pdf_path):

    try: 
         reader = PdfReader(tmp_pdf_path)
         text = ""
         for page in reader.pages:
             text += page.extract_text() or ""
         return text
    except Exception as e:
        print(f"PDF extraction error: {e}")
        return None

# Get embeddings for a list of chunks

def generate_vector_embedding(chunk):
    
    bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION)
    
    response = bedrock.invoke_model(
                    modelId=BEDROCK_EMBEDDING_MODEL,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps({
                    "texts": [chunk],
                    "input_type": "search_document"})
               )
                                        
    response = json.loads(response["body"].read())
    embedding = response.get("embeddings", [[]])[0]
    
    return embedding


# Main Lambda Handler

def lambda_handler(event, context):

    # API Gateway sends body as a string
    if isinstance(event.get("body"), str):
        body = json.loads(event["body"])
    else:
        body = event["body"]

    file_name = body["filename"]
    file_b64 = body["base64"]
    print(f"file received : {file_name}")
    
    # Convert file transmitted in base64, back to pdf to extract text from pdf.
  
    if not file_b64:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing base64 field"})
        }

    file_bytes = base64.b64decode(file_b64)

    tmp_path = f"/tmp/{file_name}.pdf"
    with open(tmp_path, "wb") as f:
        f.write(file_bytes)

    
    # 2 Extract text from pdf document
    document_type = "pdf"
    text = extract_text_from_pdf(tmp_path)

    if not text.strip():
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "No text extracted from PDF"})
        }

    
    # 3 Chunk Text with recursive text splitter with overlap of 200 for better search
    print("before splitting into text chunks: ")
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=1500,
        chunk_overlap=200
    )
    chunks = splitter.split_text(text)

   # 4 create connection to postgres/ pgvector db document chunks and embeddings
    print("before get_db_connection: ")
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        # delete from sprepo_docs table, if document is already uploaded, so new version is stored in vector db
        print("before delete from table: ")
        cur.execute(
            "DELETE FROM aone_corp_sprepo_docs WHERE document_name = %s",
            (file_name,)
        )

        deleted_rows = cur.rowcount

        if deleted_rows == 0:
            print(f"New file upload: {file_name}")
        else:
            print(f"Replacing file: {file_name} | Deleted: {deleted_rows}")

    
    # 5 Generate Embedding for each chunk

            # Generate embeddings and insert each chunk
        
        for chunk in chunks:
            #print(f"loop chunk of : {chunk}")
            embedding = generate_vector_embedding(chunk)
            chunk_id = str(uuid.uuid4())

            cur.execute(
                    """
                    INSERT INTO aone_corp_sprepo_docs 
                    (id, document_name, document_type, chunk_text, embedding) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (chunk_id, file_name, document_type, chunk, embedding)
            )
       # committing outside loop for faster response and one commit 
        print("before commit chunks to postgres DB")
        conn.commit()
           

    except Exception as e:
        conn.rollback()
        print("[ERROR] Upload failed:", str(e))
        raise

    finally:
        conn.commit()
        cur.close()
        conn.close()
    
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "PDF processed and ingested",
            "chunks": len(chunks)
        })
    }