from datetime import datetime
from collections import defaultdict
from io import BytesIO
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import logging

from reportlab.lib import colors
from reportlab.lib.pagesizes import LETTER, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak,
    Image, KeepInFrame
)
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas

try:
    from PIL import Image as PILImage  # optional; used to downscale/re-encode
except Exception:
    PILImage = None


# ---------- Canvas with page numbers ----------
class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        total_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self._draw_page_number(total_pages)
            super().showPage()
        super().save()

    def _draw_page_number(self, total_pages):
        self.setFont("Helvetica", 8)
        w, _h = self._pagesize
        self.drawRightString(w - 36, 20, f"Page {self._pageNumber} of {total_pages}")


# ---------- helpers ----------

def _txt(v):
    return "" if v is None else str(v)

def _fmt_pct(v):
    try:
        return f"{float(v):.1f}%"
    except Exception:
        return _txt(v)
    
def _fmt_money(v):
    try:
        return f"${float(v):,.2f}"
    except Exception:
        return "$0.00"

def _num(v):
    try:
        return float(v or 0)
    except Exception:
        return 0.0

def _short_name(name: str | None, limit: int = 20) -> str:
    if not name:
        return ""
    s = str(name)
    return s if len(s) <= limit else s[:limit] + "."

def _fetch_logo_bytes(logo_source: str | None) -> bytes | None:
    ## Removing Logo for troubleshooting
    
    return None
    
    # if not logo_source:
    #     return None
    # try:
    #     if logo_source.lower().startswith(("http://", "https://")):
    #         req = Request(logo_source, headers={"User-Agent": "Mozilla/5.0"})
    #         with urlopen(req, timeout=10) as resp:
    #             return resp.read()
    #     else:
    #         with open(logo_source, "rb") as f:
    #             return f.read()
    # except (HTTPError, URLError, OSError, ValueError):
    #     return None

def _prepare_logo(logo_source: str | None,
                  box_w_in: float = 1.6,
                  box_h_in: float = 1.0,
                  target_dpi: int = 150) -> tuple[bytes | None, float, float]:
    logging.info("Preparing Logo")
    """
    Return (processed_image_bytes, draw_w_pt, draw_h_pt) where draw sizes
    fit within the given inch box and preserve aspect ratio.
    If Pillow is available, downscale/re-encode to reduce PDF size.
    """
    raw = _fetch_logo_bytes(logo_source)
    if not raw:
        return None, 0.0, 0.0

    box_w_pt, box_h_pt = box_w_in * 72.0, box_h_in * 72.0

    if PILImage:
        try:
            with PILImage.open(BytesIO(raw)) as im:
                im = im.convert("RGBA") if im.mode in ("LA", "RGBA", "P") else im.convert("RGB")
                src_w, src_h = im.size
                tgt_w_px = int(box_w_in * target_dpi)
                tgt_h_px = int(box_h_in * target_dpi)
                scale = min(tgt_w_px / src_w, tgt_h_px / src_h, 1.0)  # never upscale
                new_w = max(1, int(src_w * scale))
                new_h = max(1, int(src_h * scale))
                if scale < 1.0:
                    im = im.resize((new_w, new_h), PILImage.LANCZOS)

                buf = BytesIO()
                if im.mode == "RGB":
                    im.save(buf, format="JPEG", quality=85, optimize=True, progressive=True)
                else:
                    im.save(buf, format="PNG", optimize=True)
                raw = buf.getvalue()
                px_w, px_h = new_w, new_h
        except Exception:
            # fallback: get original size from ReportLab if PIL fails
            from reportlab.lib.utils import ImageReader
            reader = ImageReader(BytesIO(raw))
            px_w, px_h = reader.getSize()
    else:
        from reportlab.lib.utils import ImageReader
        reader = ImageReader(BytesIO(raw))
        px_w, px_h = reader.getSize()

    scale_pt = min(box_w_pt / px_w, box_h_pt / px_h)
    draw_w_pt = px_w * scale_pt
    draw_h_pt = px_h * scale_pt
    return raw, draw_w_pt, draw_h_pt


# ---------- tables ----------
def _make_main_table(rows, styles):
    logging.info("Preparing Main Table")
    cell_style = ParagraphStyle("cell", parent=styles["BodyText"], fontSize=8, leading=9)
    header_style = ParagraphStyle("header", parent=styles["BodyText"], fontSize=8, leading=9)

    header = ["Unit","Tenant","Current","Guideline Rent","AGI Rent",
              "Market","Guideline Δ","AGI Δ","Notice %","Calc %","Ignored","Reason"]

    # fixed total width ~9.3"
    main_col_widths = [w*inch for w in [0.5, 1.0, 0.7, 0.8, 0.7, 0.8, 0.8, 0.7, 0.6, 0.6, 0.5, 1.6]]

    trows = [[Paragraph(h, header_style) for h in header]]
    for inc in rows:
        trows.append([
            Paragraph(inc.get("unitnumber","") or "", cell_style),
            Paragraph(_short_name(inc.get("tenantname","")), cell_style),
            Paragraph(_fmt_money(inc.get("current_rent")), cell_style),
            Paragraph(_fmt_money(inc.get("guidelinerent")), cell_style),
            Paragraph("" if inc.get("agirent") is None else _fmt_money(inc.get("agirent")), cell_style),
            Paragraph(_fmt_money(inc.get("marketrent")), cell_style),
            Paragraph(_fmt_money(inc.get("guidelineincrease")), cell_style),
            Paragraph("" if inc.get("agiincrease") is None else _fmt_money(inc.get("agiincrease")), cell_style),
            Paragraph(_fmt_pct(inc.get("percentage")), cell_style),
            Paragraph(_fmt_pct(inc.get("calculationpercentage")), cell_style),
            Paragraph(_txt(inc.get("ignored")), cell_style),
            Paragraph(_txt(inc.get("reason")), cell_style),
        ])

    tbl = Table(trows, repeatRows=1, colWidths=main_col_widths)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (2, 1), (-3, -1), "RIGHT"),
        ("ALIGN", (0, 0), (1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#EAF2FB"), colors.whitesmoke]),
    ]))
    logging.info("Completed Main Table")
    return tbl

def _make_ignored_table(rows, styles):
    logging.info("Preparing Ignored Table")
    cell_style = ParagraphStyle("cell", parent=styles["BodyText"], fontSize=8, leading=9)
    header_style = ParagraphStyle("header", parent=styles["BodyText"], fontSize=8, leading=9)

    head = ["Unit","Tenant","Reason","Current","Guideline Rent","AGI Rent","Notice %","Calc %"]
    col_widths = [w*inch for w in [0.6, 1.6, 3.2, 0.9, 1.0, 0.9, 0.55, 0.55]]

    trows = [[Paragraph(h, header_style) for h in head]]
    for inc in rows:
        trows.append([
            Paragraph(inc.get("unitnumber","") or "", cell_style),
            Paragraph(_short_name(inc.get("tenantname","")), cell_style),
            Paragraph(_txt(inc.get("reason")), cell_style),
            Paragraph(_fmt_money(inc.get("current_rent")), cell_style),
            Paragraph(_fmt_money(inc.get("guidelinerent")), cell_style),
            Paragraph("" if inc.get("agirent") is None else _fmt_money(inc.get("agirent")), cell_style),
            Paragraph(_fmt_pct(inc.get("percentage")), cell_style),
            Paragraph(_fmt_pct(inc.get("calculationpercentage")), cell_style),
        ])

    tbl = Table(trows, repeatRows=1, colWidths=col_widths)
    tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.25, colors.grey),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("ALIGN", (3, 1), (-1, -1), "RIGHT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#EAF2FB"), colors.whitesmoke]),
    ]))
    return tbl


def _building_header_with_logo(building: str, run_date: str, effective_date: str, guideline_pct: str,
                               styles, logo_source: str | None):
    # Title + meta (left)
    title = Paragraph("Rent Increase Summary", styles["Title"])
    meta_lines = [
        Paragraph(f"Run Date: {run_date}", styles["Normal"]),
        Paragraph(f"Increase Effective Date: {effective_date}", styles["Normal"]),
        Paragraph(f"Guideline Increase Rate: {guideline_pct}%", styles["Normal"]),
        Paragraph(f"Building: {building}", styles["Normal"]),
    ]
    left_block = [title, Spacer(1, 0.12 * inch), *meta_lines, Spacer(1, 0.06 * inch)]
    left = KeepInFrame(6.8 * inch, 1.2 * inch, left_block, hAlign="LEFT", vAlign="TOP")

    # Logo (right) – fit to box, preserve aspect ratio, and downscale pixels if needed
    img_bytes, draw_w_pt, draw_h_pt = _prepare_logo(logo_source, box_w_in=1.6, box_h_in=1.0, target_dpi=150)
    if img_bytes:
        logo_img = Image(BytesIO(img_bytes), width=draw_w_pt, height=draw_h_pt)
        logo_img.hAlign = "RIGHT"
    else:
        logo_img = Spacer(1, 1.0 * inch)

    header = Table([[left, logo_img]], colWidths=[6.8 * inch, 1.6 * inch])
    header.setStyle(TableStyle([
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("ALIGN", (1, 0), (1, 0), "RIGHT"),
    ]))
    return header


# ---------- main entry ----------
def build_increase_report_pdf(
    path: str,
    *,
    run_date: str,
    effective_date: str,
    guideline_pct: str,
    rows: list[dict],
    totals_by_building: dict | None = None,
    logo_source: str | None = None,
):
    logging.info("Preparing Increase Summary Report")
    """
    Multi-building PDF (one page per building) with:
      - Header + logo (top-right) on first page of each building
      - Totals box (included vs. ignored)
      - Three sections per building:
          1) Included Increases
          2) Ignored Leases (if any)
          3) All Potential Increases
    """
    pagesize = landscape(LETTER)
    doc = SimpleDocTemplate(
        path, pagesize=pagesize,
        leftMargin=36, rightMargin=36, topMargin=44, bottomMargin=36
    )
    styles = getSampleStyleSheet()
    story = []

    # Group rows by building
    by_building = defaultdict(list)
    for r in rows:
        by_building[r.get("buildingname", "Unknown Building")].append(r)
    logging.info("Group Rows by Building")

    # Compute totals if not provided
    if totals_by_building is None:
        totals_by_building = {}
        for b, rs in by_building.items():
            included = [x for x in rs if x.get("ignored") != "Y"]
            ignored = [x for x in rs if x.get("ignored") == "Y"]
            total_included = sum(_num(x.get("guidelineincrease")) + _num(x.get("agiincrease")) for x in included)
            total_ignored = sum(_num(x.get("guidelineincrease")) + _num(x.get("agiincrease")) for x in ignored)
            totals_by_building[b] = {
                "count": len(included),
                "total_inc": _fmt_money(total_included),
                "ignored_count": len(ignored),
                "ignored_total_inc": _fmt_money(total_ignored),
            }
        logging.info("Computed Totals")

    # Build pages
    for idx, (building, rs) in enumerate(by_building.items()):
        # Header + logo (only here → only on first page for this building)
        story.append(_building_header_with_logo(building, run_date, effective_date, guideline_pct, styles, logo_source))
        story.append(Spacer(1, 0.18 * inch))

        # Totals
        t = totals_by_building.get(building, {})
        tdata = [
            ["Increases (not ignored)", "Total Increase (not ignored)", "Ignored Count", "Total Ignored Increase"],
            [str(t.get("count", "")), t.get("total_inc", ""), str(t.get("ignored_count", "")), t.get("ignored_total_inc", "")],
        ]
        totals_col_widths = [w * inch for w in [2.0, 2.6, 1.8, 2.9]]  # ≈ 9.3"
        tt = Table(tdata, colWidths=totals_col_widths)
        tt.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("ALIGN", (0, 0), (-1, -1), "CENTER"),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.HexColor("#EAF2FB"), colors.whitesmoke]),
        ]))
        story.append(tt)
        story.append(Spacer(1, 0.22 * inch))

        # 1) Included increases
        included_rows = [x for x in rs if x.get("ignored") != "Y"]
        story.append(Paragraph("Included Increases", styles["Heading3"]))
        story.append(_make_main_table(included_rows, styles))
        story.append(Spacer(1, 0.22 * inch))

        # 2) Ignored leases
        ignored_rows = [x for x in rs if x.get("ignored") == "Y"]
        if ignored_rows:
            story.append(Paragraph("Ignored Leases", styles["Heading3"]))
            story.append(_make_ignored_table(ignored_rows, styles))
            story.append(Spacer(1, 0.22 * inch))

        # 3) All potential increases
        story.append(Paragraph("All Potential Increases", styles["Heading3"]))
        story.append(_make_main_table(rs, styles))

        if idx < len(by_building) - 1:
            story.append(PageBreak())

    # Build with page numbering canvas
    doc.build(story, canvasmaker=NumberedCanvas)
    logging.info("Completed Summary Report")
