from pathlib import Path
import re
import fitz  # PyMuPDF
from paddleocr import PaddleOCR

# =====================
# Config
# =====================
INPUT_DIR = Path("./data/raw_docs")
OUTPUT_IMG_DIR = Path("./output_pages")
OUTPUT_TEXT_DIR = Path("./output")
DPI = 300

OUTPUT_IMG_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_TEXT_DIR.mkdir(parents=True, exist_ok=True)

# =====================
# OCR Init
# =====================
ocr = PaddleOCR(
    use_doc_orientation_classify=False,
    use_doc_unwarping=False,
    use_textline_orientation=False,
)

# =====================
# Utils
# =====================


def clean_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def extract_pdf_to_text(pdf_path: Path) -> str:
    pdf_name = pdf_path.stem
    pdf_img_dir = OUTPUT_IMG_DIR / pdf_name
    pdf_img_dir.mkdir(parents=True, exist_ok=True)

    # ---- Render PDF pages to images
    doc = fitz.open(pdf_path)
    for page in doc:
        pix = page.get_pixmap(dpi=DPI)
        img_path = pdf_img_dir / f"page-{page.number}.png"
        pix.save(img_path)

    # ---- OCR images
    all_text: list[str] = []

    for img_path in sorted(pdf_img_dir.glob("*.png")):
        result = ocr.predict(str(img_path))
        for res in result:
            texts = res.get("rec_texts", [])
            all_text.extend(texts)

    return clean_text(" ".join(all_text))
