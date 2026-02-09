import gradio as gr
from ingest import ingest_pdf
from query import ask_question
from graph_db import get_driver

def get_doc_list():
    driver = get_driver()
    try:
        with driver.session() as session:
            result = session.run("MATCH (d:Document) RETURN d.id AS id ORDER BY d.created DESC")
            docs = [r["id"] for r in result]
        return gr.update(choices=docs, value=docs[0] if docs else None)
    finally:
        driver.close()

with gr.Blocks(title="GraphRAG Pro", theme=gr.themes.Soft()) as demo:
    gr.Markdown("# 🔍 Full Knowledge Graph RAG")

    # Global UI Elements
    with gr.Row():
        doc_dropdown = gr.Dropdown(label="Select Document to Query", choices=[], interactive=True, scale=4)
        refresh_btn = gr.Button("🔄 Refresh", scale=1)

    with gr.Tab("📄 Data Ingestion"):
        file_input = gr.File(label="Upload PDF")
        ingest_btn = gr.Button("Build Knowledge Graph", variant="primary")
        status_box = gr.Textbox(label="Processing Status", lines=3)

    with gr.Tab("💬 Knowledge Chat"):
        msg_input = gr.Textbox(label="Question", placeholder="How are the entities in this doc related?")
        ans_output = gr.Textbox(label="Graph-Augmented Answer", lines=10)
        ask_btn = gr.Button("Search Graph", variant="primary")

    # Event Mapping
    def start_ingest_msg(): return "🚀 Processing... Check terminal for chunk progress."
    
    ingest_btn.click(fn=start_ingest_msg, outputs=status_box).then(
        fn=ingest_pdf, inputs=file_input, outputs=None
    ).then(
        fn=lambda: "✅ Ingestion Complete! All nodes and relations stored.", outputs=status_box
    ).then(
        fn=get_doc_list, outputs=doc_dropdown
    )

    ask_btn.click(fn=ask_question, inputs=[msg_input, doc_dropdown], outputs=ans_output)
    refresh_btn.click(fn=get_doc_list, outputs=doc_dropdown)
    
    demo.load(get_doc_list, outputs=doc_dropdown)

if __name__ == "__main__":
    demo.queue().launch()
