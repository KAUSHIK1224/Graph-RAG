import os
from dotenv import load_dotenv
from neo4j import GraphDatabase
from langchain_ollama import ChatOllama

# Load environment variables
load_dotenv()

NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USERNAME = os.getenv("NEO4J_USERNAME")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")
LLAMA_PARSE_KEY = os.getenv("LLAMA_CLOUD_API_KEY")

if not all([NEO4J_URI, NEO4J_USERNAME, NEO4J_PASSWORD]):
    raise ValueError("❌ Missing Neo4j credentials in .env")

def get_driver():
    return GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USERNAME, NEO4J_PASSWORD))

def get_llm():
    return ChatOllama(
        model="llama3.1:latest",
        temperature=0,
        num_ctx=8192,
        num_predict=1024
    )