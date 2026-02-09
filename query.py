from graph_db import get_driver, get_llm

def ask_question(question: str, doc_id: str):
    print(f"\n🔍 Querying Doc: {doc_id} for '{question}'")
    
    driver = get_driver()
    llm = get_llm()
    
    # INITIALIZE CONTEXT HERE to avoid crash
    context = ""

    try:
        with driver.session() as session:
            # 1. Get Text Chunks
            chunk_query = """
            MATCH (d:Document {id:$doc_id})-[:HAS_CHUNK]->(c:Chunk)
            RETURN c.text AS text
            ORDER BY c.index ASC
            LIMIT 5
            """
            chunk_rows = session.run(chunk_query, doc_id=doc_id).data()

            # 2. Get Graph (Improved Search)
            graph_query = """
            MATCH (d:Document {id:$doc_id})-[:HAS_CHUNK]->(c:Chunk)-[:MENTIONS]->(e:Entity)
            MATCH (e)-[r]-(target:Entity)
            // We want the relationships BETWEEN entities, so we filter out metadata
            WHERE NOT type(r) IN ['HAS_CHUNK', 'HAS_DOCUMENT', 'MENTIONS'] 
            RETURN DISTINCT 
                   e.id AS source, 
                   type(r) AS rel_type, 
                   target.id AS target,
                   e.type AS source_label,
                   target.type AS target_label
            LIMIT 100
            """
            graph_rows = session.run(graph_query, doc_id=doc_id).data()

        # Build Context from Text Chunks
        if chunk_rows:
            context += "### TEXT CONTENT ###\n"
            for r in chunk_rows:
                context += f"{r['text']}\n---\n"
        
        # Build Context from Graph
        if graph_rows:
            context += "\n### KNOWLEDGE GRAPH (Key Entities & Relationships) ###\n"
            for r in graph_rows:
                # Use .get() to avoid errors if a label is missing
                s_label = r.get('source_label', 'Entity')
                t_label = r.get('target_label', 'Entity')
                context += f"({r['source']}:{s_label}) -[:{r['rel_type']}]-> ({r['target']}:{t_label})\n"
        
        if not context:
            return "❌ No data found for this document in the database."

        # Final Prompt
        prompt = f"""You are a helpful assistant. Answer the question using the context below.
        If the Knowledge Graph contains information that contradicts or expands on the Text Content, 
        prioritize the structured Knowledge Graph data.

Context:
{context}

Question: {question}

Answer:"""

        print(f"🤖 Context size: {len(context)} chars. Generating answer...")
        response = llm.invoke(prompt)
        return response.content

    except Exception as e:
        print(f"❌ Query Error: {str(e)}")
        return f"❌ Error: {str(e)}"
    finally:
        driver.close()
