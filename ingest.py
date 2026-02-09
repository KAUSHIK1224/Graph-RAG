import os
import hashlib
from llama_parse import LlamaParse
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_experimental.graph_transformers import LLMGraphTransformer
from graph_db import get_driver, get_llm, LLAMA_PARSE_KEY

def make_doc_id(path: str):
    name = os.path.basename(path).replace(" ", "_").replace(".pdf", "")
    h = hashlib.md5(path.encode()).hexdigest()[:6]
    return f"{name}_{h}"

def ingest_pdf(file_path: str):
    if not os.path.exists(file_path):
        print(f"❌ File not found: {file_path}")
        return None

    doc_id = make_doc_id(file_path)
    doc_name = os.path.basename(file_path)
    driver = get_driver()
    llm = get_llm()
    
    try:
        print(f"📄 Processing: {doc_name} (ID: {doc_id})")

        # 1. Document & Chunk Storage
        with driver.session() as session:
            # Check if exists
            exists = session.run("MATCH (d:Document {id:$id}) RETURN count(d) > 0 AS exists", id=doc_id).single()["exists"]
            if exists:
                print(f"⚠️ Document already ingested.")
                return doc_id
            
            session.run("CREATE (d:Document {id:$id, source:$src, type:'pdf', created:datetime()})", id=doc_id, src=doc_name)

        # 2. Parse PDF
        print("📖 Parsing PDF...")
        parser = LlamaParse(api_key=LLAMA_PARSE_KEY, result_type="text")
        pages = parser.load_data(file_path)
        if not pages: return None

        # 3. Split into Chunks
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)
        full_text = "\n".join([p.text for p in pages])
        docs = [Document(page_content=full_text, metadata={"doc_id": doc_id})]
        chunks = text_splitter.split_documents(docs)

        # 4. Extract Graph
        print(f"🧠 Extracting Graph from {len(chunks)} chunks...")
        graph_transformer = LLMGraphTransformer(llm=llm)
        
        for i, chunk in enumerate(chunks):
            try:
                # IMPORTANT: LLMGraphTransformer.convert_to_graph_documents returns a list 
                # where each element is a GraphDocument.
                res = graph_transformer.convert_to_graph_documents([chunk])
                if not res: continue
                
                nodes = res[0].nodes
                rels = res[0].relationships
                chunk_id = f"{doc_id}_chunk_{i}"

                with driver.session() as session:
                    # Save Chunk first
                    session.run("""
                        MATCH (d:Document {id:$doc_id})
                        MERGE (c:Chunk {id: $cid})
                        SET c.text = $text, c.index = $idx
                        MERGE (d)-[:HAS_CHUNK]->(c)
                    """, doc_id=doc_id, cid=chunk_id, text=chunk.page_content, idx=i)

                    # PHASE 1: MERGE ENTITIES
                    for node in nodes:
                        clean_id = str(node.id).strip().replace('"', '')
                        session.run("""
                            MERGE (e:Entity {id: $eid})
                            SET e.type = $etype
                            WITH e
                            CALL apoc.create.addLabels(e, [$etype]) YIELD node
                            RETURN count(*)
                        """, eid=clean_id, etype=node.type)
                        
                        # Link Entity to Chunk
                        session.run("MATCH (c:Chunk {id:$cid}), (e:Entity {id:$eid}) MERGE (c)-[:MENTIONS]->(e)",
                                    cid=chunk_id, eid=clean_id)

                    # PHASE 2: MERGE RELATIONSHIPS
                    for rel in rels:
                        src = str(rel.source.id).strip().replace('"', '')
                        tgt = str(rel.target.id).strip().replace('"', '')
                        rtype = rel.type.replace(" ", "_").upper().strip()
                        session.run(f"""
                            MATCH (a:Entity {{id: $src}})
                            MATCH (b:Entity {{id: $tgt}})
                            MERGE (a)-[:`{rtype}`]->(b)
                        """, src=src, tgt=tgt)

                print(f"   ✅ Chunk {i+1}: Saved {len(nodes)} entities")
            except Exception as e:
                print(f"   ⚠️ Error processing chunk {i}: {e}")

        print("✅ Ingestion Complete.")
        return doc_id

    except Exception as e:
        print(f"❌ Critical Error: {e}")
        return None
    finally:
        driver.close()
import os
import hashlib
from llama_parse import LlamaParse
from langchain_core.documents import Document
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_experimental.graph_transformers import LLMGraphTransformer
from graph_db import get_driver, get_llm, LLAMA_PARSE_KEY

def make_doc_id(path: str):
    name = os.path.basename(path).replace(" ", "_").replace(".pdf", "")
    h = hashlib.md5(path.encode()).hexdigest()[:6]
    return f"{name}_{h}"

def ingest_pdf(file_path: str):
    doc_id = make_doc_id(file_path)
    doc_name = os.path.basename(file_path)
    driver = get_driver()
    llm = get_llm()
    
    try:
        # 1. Initialize Document
        with driver.session() as session:
            session.run("MERGE (d:Document {id:$id}) SET d.source=$src, d.created=datetime()", 
                        id=doc_id, src=doc_name)

        # 2. Parse and Chunk
        parser = LlamaParse(api_key=LLAMA_PARSE_KEY, result_type="text")
        pages = parser.load_data(file_path)
        full_text = "\n".join([p.text for p in pages])
        
        # Using smaller chunks to ensure the LLM doesn't miss entities
        text_splitter = RecursiveCharacterTextSplitter(chunk_size=2000, chunk_overlap=200)
        chunks = text_splitter.split_documents([Document(page_content=full_text)])

        # 3. Extract Graph
        graph_transformer = LLMGraphTransformer(llm=llm)
        
        for i, chunk in enumerate(chunks):
            # Extracting from the current chunk
            graph_docs = graph_transformer.convert_to_graph_documents([chunk])
            if not graph_docs: continue
            
            # The transformer returns a list of GraphDocuments
            nodes = graph_docs[0].nodes
            rels = graph_docs[0].relationships
            chunk_id = f"{doc_id}_chunk_{i}"

            with driver.session() as session:
                # Save Chunk text
                session.run("""
                    MATCH (d:Document {id:$doc_id}) 
                    MERGE (c:Chunk {id:$cid}) 
                    SET c.text=$t 
                    MERGE (d)-[:HAS_CHUNK]->(c)
                """, doc_id=doc_id, cid=chunk_id, t=chunk.page_content)

                # --- PHASE 1: Create all Nodes first ---
                for node in nodes:
                    clean_id = str(node.id).strip().replace('"', '')
                    # We use apoc to set the specific LLM-extracted label (e.g., :Person)
                    session.run(f"""
                        MERGE (e:Entity {{id: $eid}}) 
                        SET e.type = $etype 
                        WITH e 
                        CALL apoc.create.addLabels(e, [$etype]) YIELD node 
                        RETURN count(*)
                    """, eid=clean_id, etype=node.type)
                    
                    # Link Chunk to the Entity it mentions
                    session.run("MATCH (c:Chunk {id:$cid}), (e:Entity {id:$eid}) MERGE (c)-[:MENTIONS]->(e)",
                                cid=chunk_id, eid=clean_id)

                # --- PHASE 2: Create Relationships ---
                for rel in rels:
                    src = str(rel.source.id).strip().replace('"', '')
                    tgt = str(rel.target.id).strip().replace('"', '')
                    rtype = rel.type.replace(" ", "_").upper()
                    
                    # Merge relationship between the two entities
                    session.run(f"""
                        MATCH (a:Entity {{id:$src}}) 
                        MATCH (b:Entity {{id:$tgt}}) 
                        MERGE (a)-[:`{rtype}`]->(b)
                    """, src=src, tgt=tgt)
                    
        return doc_id
    except Exception as e:
        print(f"Extraction Error: {e}")
        return None
    finally:
        driver.close()
