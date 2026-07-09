# =============================================================
# CONVERSOR — Markdown para PDF
#
# RESUMO: Converte o arquivo documentacao_tecnica.md para PDF
# usando pdfkit + wkhtmltopdf.
# =============================================================

import markdown
import pdfkit
import os

# Caminho do wkhtmltopdf — ajusta se necessário
WKHTMLTOPDF_PATH = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'

# Lê o arquivo Markdown
print('📖 Lendo documentacao_tecnica.md...')
with open('docs/documentacao_tecnica.md', 'r', encoding='utf-8') as f:
    conteudo_md = f.read()

# Converte Markdown para HTML
print('🔄 Convertendo Markdown para HTML...')
html_content = markdown.markdown(
    conteudo_md,
    extensions=[
        'markdown.extensions.tables',
        'markdown.extensions.fenced_code',
        'markdown.extensions.toc',
    ]
)

# CSS para deixar o PDF bonito
css = """
<style>
    body {
        font-family: Arial, sans-serif;
        font-size: 12px;
        line-height: 1.6;
        color: #333;
        max-width: 800px;
        margin: 0 auto;
        padding: 20px;
    }
    h1 { color: #1a1a2e; border-bottom: 3px solid #e94560; padding-bottom: 10px; }
    h2 { color: #16213e; border-bottom: 1px solid #ddd; padding-bottom: 5px; margin-top: 30px; }
    h3 { color: #0f3460; }
    table {
        border-collapse: collapse;
        width: 100%;
        margin: 15px 0;
    }
    th {
        background-color: #1a1a2e;
        color: white;
        padding: 8px;
        text-align: left;
    }
    td { padding: 8px; border: 1px solid #ddd; }
    tr:nth-child(even) { background-color: #f9f9f9; }
    code {
        background-color: #f4f4f4;
        padding: 2px 5px;
        border-radius: 3px;
        font-family: monospace;
        font-size: 11px;
    }
    pre {
        background-color: #f4f4f4;
        padding: 15px;
        border-radius: 5px;
        border-left: 4px solid #e94560;
    }
    blockquote {
        border-left: 4px solid #e94560;
        margin: 10px 0;
        padding: 10px 20px;
        background-color: #fff3f3;
        color: #666;
    }
    img {
        max-width: 100%;
        height: auto;
        margin: 10px 0;
        border: 1px solid #ddd;
        border-radius: 5px;
    }
    hr { border: none; border-top: 1px solid #ddd; margin: 20px 0; }
</style>
"""

# Monta o HTML completo
html_final = f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    {css}
</head>
<body>
    {html_content}
</body>
</html>
"""

# Salva o HTML temporário
print('💾 Salvando HTML temporário...')
caminho_html = os.path.abspath('docs/temp_doc.html')
with open(caminho_html, 'w', encoding='utf-8') as f:
    f.write(html_final)

# Converte HTML para PDF
print('📄 Convertendo para PDF...')
config = pdfkit.configuration(wkhtmltopdf=WKHTMLTOPDF_PATH)
caminho_pdf = os.path.abspath('docs/documentacao_tecnica.pdf')

pdfkit.from_file(
    caminho_html,
    caminho_pdf,
    configuration=config,
    options={
        'encoding': 'UTF-8',
        'enable-local-file-access': None,
        'margin-top': '15mm',
        'margin-bottom': '15mm',
        'margin-left': '15mm',
        'margin-right': '15mm',
    }
)

# Remove o HTML temporário
os.remove(caminho_html)

print(f'✅ PDF gerado: {caminho_pdf}')
print('🎉 Conversão concluída!')