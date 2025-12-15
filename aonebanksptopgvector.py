# Lambda function aonebanksptopgvector Code to ingest sharepoint documents to PGVector aonebanksptopgvector
# Author: Srinivasan Meka
# Creation Date: 18-NOV-2025
# Updated Date: 14-DEC-2025
import os
import base64
import boto3
import json
import requests
import msal
import psycopg
import io 

# import libraries for PFX to PEM conversion
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.hazmat.primitives.asymmetric import rsa

from office365.runtime.auth.client_credential import ClientCredential
from office365.sharepoint.client_context import ClientContext
from office365.runtime.auth.user_credential import UserCredential
from office365.sharepoint.files.file import File
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pgvector.psycopg import register_vector
from botocore.exceptions import ClientError
import uuid
from pypdf import PdfReader
# import traceback

from datetime import datetime, timezone

SECRET_NAME = "rds-db-credentials/cluster-CZRGZG2DXJGCQGM4OZTEHRR2SU/postgres/1762906251764"
BEDROCK_EMBEDDING_MODEL = "cohere.embed-english-v3"
AWS_REGION = "us-east-1"

# Use the credentials from your logs/Secrets Manager
secretsmanager = boto3.client('secretsmanager')

# Secret Names (replace with your actual secret names)
CERT_SECRET_NAME = "aone/sharepoint/connection"
CONFIG_SECRET_NAME = "aonebank/cert-config"
CERT_FILE_PATH = "/tmp/aonespreg-cert.pfx"

# Paths for the new PEM files
CERT_PEM_PATH = "/tmp/cert.pem"
KEY_PEM_PATH = "/tmp/key.pem"



def get_all_files(ctx, folder_relative_url):
   
    all_files = []
    print(f"inside get_all_files for {folder_relative_url}")
    try:
        folder = ctx.web.get_folder_by_server_relative_url(folder_relative_url)
        ctx.load(folder)
        ctx.execute_query()

        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        print(f"Found {len(files)} files in {folder_relative_url}")
        for file in files:
            file_name = file.properties.get("Name")
            file_url = file.properties.get("ServerRelativeUrl")

            try:
                file_response = File.open_binary(ctx, file_url)
                content = file_response.content
                size = len(content) if content else 0
                print(f"corporate repo policy doc details {file_name} ({size} bytes)")

                all_files.append({
                    "name": file_name,
                    "url": file_url,
                    "content": content
                })
            except Exception as fe:
                print(f" Loop of files not read {file_name}: {fe}")

        return all_files

    except Exception as e:
        print(f" Error exception of get_all_files for  {folder_relative_url}: {e}")
        return []

def list_files_in_folder(ctx: ClientContext, folder_url: str, depth=0):
    """Recursively list all files in a SharePoint folder."""
    print("inside list_files_in_folder")
    indent = "  " * depth
    try:
        folder = ctx.web.get_folder_by_server_relative_url(folder_url)
        ctx.load(folder)
        ctx.execute_query()

        files = folder.files
        ctx.load(files)
        ctx.execute_query()

        for file in files:
            print(f"{indent}?? {file.properties['Name']}")

    except Exception as e:
        print(f" Error accessing folder {folder_url}: {e}")

def get_sp_secrets():
    """Retrieves SharePoint certificate (Base64-encoded) and configuration secrets from AWS Secrets Manager,
    converts the PFX to PEM, and returns config values.
    """
    print("inside get_sp_secrets before cert_response and cert_binary")

    try:
        # Retrieve Base64-encoded PFX certificate
        cert_response = secretsmanager.get_secret_value(SecretId="aone/sharepoint/connection")
        secret_data = json.loads(cert_response['SecretString'])  # Parse JSON
        cert_b64 = secret_data['sp_secret']  # make sure this matches the key in the secret
        cert_b64_clean = "".join(cert_b64.split())  # remove whitespace/newlines
        cert_binary = base64.b64decode(cert_b64_clean)

        print(f"DIAGNOSTIC: Decoded PFX Binary size: {len(cert_binary)} bytes")

        #  Retrieve client_id, tenant_id, cert_password from config secret
        config_response = secretsmanager.get_secret_value(SecretId="aonebank/cert-config")
        config_data = json.loads(config_response['SecretString'])
        cert_password = config_data['cert_password'].strip()
        print(f"DIAGNOSTIC: Stripped Password string length: {len(cert_password)}")

    except Exception as e:
        print(f"FATAL ERROR: Could not retrieve or decode secrets: {e}")
        raise e

    # Load and decrypt PFX to PEM
    try:
        private_key, certificate, ca_certificates = pkcs12.load_key_and_certificates(
            cert_binary,
            cert_password.encode('utf-8'),
            default_backend()
        )
    except Exception as e:
        print(f"ERROR: Failed to load PFX: {e}")
        raise e

    print("after pkcs12.load_key_and_certificates (success)")

    # Write PEM files to /tmp
    KEY_PEM_PATH = "/tmp/key.pem"
    CERT_PEM_PATH = "/tmp/cert.pem"

    with open(KEY_PEM_PATH, "wb") as kf:
        kf.write(
            private_key.private_bytes(
                encoding=serialization.Encoding.PEM,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )
        )

    with open(CERT_PEM_PATH, "wb") as cf:
        cf.write(certificate.public_bytes(serialization.Encoding.PEM))

    print("AUTH_DEBUG: PFX successfully converted to PEM files.")

    # Return all values needed in main Lambda
    config_data['key_path'] = KEY_PEM_PATH
    config_data['cert_path'] = CERT_PEM_PATH
    return config_data

# get PGVector credentials from AWS Secrets Manager 
def get_secret():
    client = boto3.client("secretsmanager", region_name=AWS_REGION)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=SECRET_NAME)
    except ClientError as e:
        raise Exception(f"Could not retrieve secret: {e}")
    return get_secret_value_response["SecretString"]

# Connect to PostgreSQL and register pgvector
def get_db_connection():

    print("inside get_db_connection")
    secret_dict = json.loads(get_secret())
    print("inside get_db_connection after get_secret before connect")
        
    conn = psycopg.connect(
        host=secret_dict["host"],
        dbname="aonebankpgvdb",  
        user=secret_dict["username"],
        password=secret_dict["password"],
        port=secret_dict["port"]
    )

    print("Before register_vector")
    register_vector(conn)
    return conn
    

def extract_text_from_pdf(file_content_bytes):
    """Extracts text from PDF bytes using pypdf."""
    try:
        # pypdf expects a file-like object, so we wrap the bytes
        reader = PdfReader(io.BytesIO(file_content_bytes)) 
        text = ""
        for page in reader.pages:
            text += page.extract_text() or "" # Added or "" to handle empty pages gracefully
        return text
    except Exception as e:
        print(f"PDF extraction error: {e}")
        return None

def extract_text_from_docx(file_content_bytes):

    print("Word document extraction when ready.")
    return "DOCX Word document extraction when ready using python-docx."

# Read sharepoint URL and read documents to store chunks into pgvector table
def ingest_documents(ctx: ClientContext, document_library_title: str, folder_relative_path: str):
    
    conn = None

    print("Beginning ingest_documents")

    # Initialize Postgres and PGVector inside get_db_connection
    conn = get_db_connection()
    dbcursor = conn.cursor()

    bedrock = boto3.client(service_name='bedrock-runtime', region_name=AWS_REGION)  
    text_splitter = RecursiveCharacterTextSplitter(
                           chunk_size=1500,
                           chunk_overlap=200,
                           length_function=len)

    # Load SharePoint context
    web = ctx.web
    ctx.load(web.webs)
    ctx.load(web.folders)
    ctx.execute_query()

    print("Top-level folders:")
    for folder in web.folders:
        print(" -", folder.properties["ServerRelativeUrl"])

    # Define folder path
    site_relative_base = "/sites/CorporateRegulatoryGovernanceandPolicyPortal"
    if folder_relative_path:
        relative_url = folder_relative_path
    else:
        relative_url = f"{site_relative_base}/{document_library_title}"

    site_path = f"{site_relative_base}/Corporate Governance and Policy"
    list_files_in_folder(ctx, site_path)

    # Get all files from SharePoint folder
    folder_relative_url = f"{site_relative_base}/Corporate Governance and Policy"
    all_files = get_all_files(ctx, folder_relative_url)
    print(f"Found {len(all_files)} files in {folder_relative_url}")

    # Process each file
    for f in all_files:
        filename = f["name"]
        file_content_bytes = f["content"]
        print(f"\nProcessing file: {filename}")

        try:
            # Detect file extension
            file_ext = os.path.splitext(filename)[1].lower()

            # Extract text
            if file_ext == ".pdf":
                content = extract_text_from_pdf(file_content_bytes)
                document_type = "pdf"
            elif file_ext in (".docx", ".doc"):
                content = extract_text_from_docx(file_content_bytes)
                document_type = "docx"
            else:
                print(f"Skipping unsupported file type: {file_ext}")
                continue

            if not content:
                print(f"Skipping {filename}: Could not extract text.")
                continue

            # Split into chunks
            chunks = text_splitter.split_text(content)
            print(f"Split {filename} into {len(chunks)} chunks.")

            # Generate embeddings and insert each chunk
            for chunk in chunks:
                response = bedrock.invoke_model(
                    modelId=BEDROCK_EMBEDDING_MODEL,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps({
                    "texts": [chunk],
                    "input_type": "search_document"})
                )
                                        
                response_body = json.loads(response["body"].read())
                embedding = response_body.get("embeddings", [[]])[0]


                # Insert into pgvector
                chunk_id = str(uuid.uuid4())
                dbcursor.execute(
                    """
                    INSERT INTO aone_corp_sprepo_docs 
                    (id, document_name, document_type, chunk_text, embedding) 
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (chunk_id, filename, document_type, chunk, embedding)
                )

            conn.commit()
            print(f"Successfully ingested: {filename}")

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            conn.rollback()
            continue  # Move on to next file

    # Close database connections
    dbcursor.close()
    conn.close()
    print("  files processed and stored in pgvector.")

# AWS Lambda handle main function
def lambda_handler(event, context):

    try:
        print("beginning of lambda_handler execution")

        # Reading secrets and certificate from aws secrets manager get_sp_secrets()
        # Check if the PEM files exist (warm start check)
        if not os.path.exists(CERT_PEM_PATH) or not os.path.exists(KEY_PEM_PATH):
            print("AUTH_DEBUG: Cold Start - Retrieving and converting PFX to PEM in Secrets Manager")
            config = get_sp_secrets() # Retrieves, converts, and writes PEM files
        else:
            # If the Lambda container is re-used (warm start), we only need to read the config
            print("AUTH_DEBUG: Warm Start - Reading config only (PEM files should already exist).")
            config_response = secretsmanager.get_secret_value(SecretId=CONFIG_SECRET_NAME)
            config = json.loads(config_response['SecretString'])

        # Get necessary values from the retrieved config
        app_client_id = config['client_id']
        cert_password = config['cert_password']
        tenant_id = config['tenant_id']
        site_url = "https://aonebank.sharepoint.com/sites/CorporateRegulatoryGovernanceandPolicyPortal"

        print(f"AUTH_DEBUG: Config loaded. Client ID: {app_client_id[:8]}... , Tenant ID: {tenant_id}")
        
        # Diagnostic print to confirm files exist
        if os.path.exists(KEY_PEM_PATH):
             with open(KEY_PEM_PATH, "r") as f:
                 print(f"KEY PEM preview (first 100 chars):\n{f.read(100)}")
        
        if os.path.exists(CERT_PEM_PATH):
             with open(CERT_PEM_PATH, "r") as f:
                 print(f"CERT PEM preview (first 100 chars):\n{f.read(100)}")

        # MSAL AUTHENTICATION and 
        # MSAL Configuration
        AUTHORITY = f"https://login.microsoftonline.com/{tenant_id}"
        # NOTE: Ensure this thumbprint matches the one uploaded to your Azure App Registration
        CERT_THUMBPRINT = "3E73BC40FAD9CAEFFDCB3FD40AA993E935E5BFF4"
        SCOPE = ["https://aonebank.sharepoint.com/.default"]

        # Read the private key content from the PEM file
        with open(KEY_PEM_PATH, "r") as key_file:
            private_key_content = key_file.read()

        # Initialize MSAL Confidential Client
        app = msal.ConfidentialClientApplication(
            app_client_id,
            authority=AUTHORITY,
            client_credential={
                "thumbprint": CERT_THUMBPRINT,
                "private_key": private_key_content,
            }
        )
        print("AUTH_DEBUG: MSAL Confidential Client initialized.")
        
        # Acquire the token
        result = app.acquire_token_for_client(scopes=SCOPE)

        # Sharepoint context and getting MSAL token after authentication is successful 
        if "access_token" in result:
            access_token = result['access_token']
            
            # Extract expires_on (Unix timestamp) from MSAL result for the client library
            expires_on = result.get('expires_on')
            
            # Create a class to hold token information including expiration
            class AccessTokenInfo(object):
                def __init__(self, token_type, access_token, expires_on):
                    # SharePoint client expects these exact attribute names
                    self.tokenType = token_type 
                    self.accessToken = access_token
                    self.expires_on = expires_on 

            # Define the callable function that returns the structured token object
            token_callable = lambda: AccessTokenInfo("Bearer", access_token, expires_on)

            ctx = ClientContext(site_url).with_access_token(token_callable)
            print("AUTH_DEBUG: Connection context created using MSAL token.")


            #ctx = ClientContext(site_url).with_access_token(access_token)

# TIMEZONE format MSAL Library token_expires converting to timezone format. 
            try:
                auth_ctx = getattr(ctx, "authentication_context", None)
                if auth_ctx and hasattr(auth_ctx, "_token_expires"):
                    expires_val = auth_ctx._token_expires
                    if isinstance(expires_val, str):
                        auth_ctx._token_expires = datetime.fromisoformat(
                            expires_val.replace("Z", "+00:00")
                        )
                    elif expires_val and expires_val.tzinfo is None:
                        auth_ctx._token_expires = expires_val.replace(tzinfo=timezone.utc)
            except Exception as tz_fix_err:
                print(f"WARNING: token expiry timezone fix skipped: {tz_fix_err}")

            # Test connection (First query)
            web = ctx.web
            ctx.load(web, ["Title", "Url"]) # Use minimal properties for safety
            ctx.execute_query()
            
            # Use web.title (lowercase) instead of web.Title
            print(f"SUCCESS: Connected to SharePoint site: {web.title} ({web.url})")

            # Define ingestion parameters
            # NOTE: Assuming 'Corporate Governance and Policy' is the document library title
            document_library = 'Shared Documents'
            # NOTE: Assuming you want to read files directly in this library's root, if not, set folder_relative_path
            folder_relative_path = "/sites/CorporateRegulatoryGovernanceandPolicyPortal/Corporate Governance and Policy"
            
            # Fix the timezone mismatch in the Office365 token expiration
            if getattr(ctx, "authentication_context", None) and hasattr(ctx.authentication_context, "_token_expires"):
               exp = ctx.authentication_context._token_expires
               if exp.tzinfo is None:  # make it UTC-aware
                  ctx.authentication_context._token_expires = exp.replace(tzinfo=timezone.utc)
            
            document_library_title = "/sites/CorporateRegulatoryGovernanceandPolicyPortal/Corporate Governance and Policy"
            folder_path = ""
            document_library = ""
            folder_relative_path = "/sites/CorporateRegulatoryGovernanceandPolicyPortal/Corporate Governance and Policy"  # or e.g., "Corporate Governance and Policy/SubfolderName"

            print("Beginning ingest_documents")
            ingest_documents(ctx, "Corporate Governance and Policy","/sites/CorporateRegulatoryGovernanceandPolicyPortal/Corporate Governance and Policy")

            return {"statusCode": 200, "body": "Ingestion successful"}

        else:
            # Handle MSAL acquisition failure (e.g., bad client ID, wrong scope, expired cert)
            error_details = json.dumps(result, indent=2)
            print(f"FATAL ERROR: MSAL failed to acquire token. Details:\n{error_details}")
            # Raising an exception here will trigger the outer 'except' block below
            raise Exception("Token acquisition failed via MSAL.")

    #  exception handler for the entire main Lambda
    except Exception as e:
        # Import traceback locally to prevent shadowing/global errors
        import sys, traceback # Local import is safer here
        
        print(f"ERROR: Unhandled exception during Lambda execution: {e}")
        # Log the full traceback for better debugging
        traceback.print_exc()
        return {"statusCode": 500, "body": f"Ingestion failed: {e}"}
