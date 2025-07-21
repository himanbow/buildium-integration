from PyPDF2 import PdfReader, PdfWriter
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import letter
import io
import datetime
import os

today = datetime.datetime.today().strftime("%d/%m/%Y")

def formatdate(date):
    # Parse the input string into a datetime object
    date = datetime.datetime.strptime(date, "%Y-%m-%d")
    # Format the datetime object into the desired string format
    date = date.strftime("%d / %m / %Y")
    
    return date

def create_text_overlay(data, overlay_path):
    """Create a text overlay with the provided data at specified coordinates."""
    global today
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=letter)
    

    # First page content
    c.setFont("Helvetica", 9)
    c.drawString(31, 671, data['alltenantnames'])  # Tenant's name
    c.drawString(31, 652, data['address'])  # Tenant's address
    c.drawString(31, 608, data['address'])  # Repeated address if required
    c.setFont("Helvetica", 12)
    c.drawString(460, 547, f"{formatdollaramount(data['newrent'])}")  # New rent amount
    c.drawString(191, 547, formatdate(data['increasedate']))  # Increase effective date
    c.drawString(280, 406, f"{formatdollaramount(data['increase'])}")  # Increase amount
    c.drawString(500, 368, f"{data['percentage']}")  # Percentage increase

    if data['agitype'] is None:
        c.drawString(128, 301, "X")
        
    else:
        c.drawString(128, 228, "X") 
        if data['agitype'] == "Approved":
            c.drawString(162, 193, "X")
        if data['agitype'] == "Not Approved":
            c.drawString(162, 148, "X")   

    # End of first page
    c.showPage()  # End the first page and move to the second page

    # Second page content
    c.setFont("Helvetica", 14)
    today = datetime.datetime.today().strftime("%d / %m / %Y")
    c.drawString(320, 287, today)  # Today's date on the second page

    # Save the overlay to a temporary path
    c.save()
    packet.seek(0)

    with open(overlay_path, 'wb') as f:
        f.write(packet.getvalue())

def merge_pdfs(original_pdf, overlay_pdf, output_pdf):
    """Merge the text overlay with the original PDF."""
    reader = PdfReader(original_pdf)
    overlay_reader = PdfReader(overlay_pdf)
    writer = PdfWriter()

    # Merge overlay with each page
    for i, page in enumerate(reader.pages):
        overlay_page = overlay_reader.pages[i]  # Use the corresponding overlay page
        page.merge_page(overlay_page)
        writer.add_page(page)

    # Save the merged PDF to the output path
    with open(output_pdf, 'wb') as f:
        writer.write(f)

def formatdollaramount(amount):
    amount = '{:,.2f}'.format(amount)
    dollar_amount = ' '.join(str(amount))
    return dollar_amount


async def create(leaseid, data):

    address = data['address']

    address = address.split(',', 1)[0]

    datename = datetime.datetime.strptime(data['increasedate'], "%Y-%m-%d")
    datename = datename.strftime("%B %d, %Y")
    # Input paths and temporary paths
    input_pdf_path = '/tmp/N1.pdf'
    overlay_pdf_path = f'/tmp/N1 for Apartment {address} Effective {datename}_overlay.pdf'
    output_pdf_path = f'/tmp/N1 for Apartment {address} Effective {datename}.pdf'

    # Create the overlay with text at specified coordinates
    create_text_overlay(data, overlay_pdf_path)

    # Merge the overlay onto the original PDF
    merge_pdfs(input_pdf_path, overlay_pdf_path, output_pdf_path)

    # Remove the temporary overlay file after merging
    os.remove(overlay_pdf_path)

    return output_pdf_path


async def create_summary_page(summary_data, buildingname, countbuilding, date):
    """Create a summary page and return the BytesIO object containing the summary."""
    packet = io.BytesIO()
    c = canvas.Canvas(packet, pagesize=letter)

    # --- normalize your incoming `date` parameter ---
    # Try ISO first, then human‐readable
    try:
        dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        dt = datetime.datetime.strptime(date, "%B %d, %Y")
    effective_date_str = dt.strftime("%B %d, %Y")

    # today label
    today_str = datetime.datetime.today().strftime("%B %d, %Y")
    countbuilding = str(countbuilding)

    # Header
    c.setFont("Helvetica-Bold", 12)
    c.drawString(100, 750, "N1 Increase Notice Summary")
    c.drawString(100, 735, buildingname)
    c.setFont("Helvetica", 7)
    c.drawString(50, 710, f"Number Of Increases: {countbuilding}")
    c.drawString(50, 700, f"Generated on: {today_str}")
    c.drawString(50, 690, f"Increases Effective: {effective_date_str}")

    # Column Headers
    c.setFont("Helvetica-Bold", 9)
    c.drawString(30, 670, "Tenant Names")
    c.drawString(130, 670, "Rent Charge")
    c.drawString(200, 670, "Other Charges")
    c.drawString(270, 670, "Total Charges")
    c.drawString(350, 670, "Increase")
    c.drawString(430, 670, "Unit")
    c.drawString(510, 670, "Delivered")
    c.setFont("Helvetica", 8)
    c.drawString(325, 740, "Notices Delivered By:__________________________________")
    c.drawString(325, 690, "Date:_______________ Delivery Method:__________________")

    # Add data rows
    y = 650
    c.setFont("Helvetica", 9)
    for data in summary_data:
        tenant_names = data['increasenotice'].get('alltenantnames', 'N/A')
        rent_charge = data['increasenotice'].get('newrent', 0)
        formatted_rent_charge = f"${rent_charge:,.2f}"
        unit = data['increasenotice'].get('unit', 'N/A')

        # Extract other charges
        charges = data.get('renewal', {}).get('Rent', {}).get('Charges', [])
        other_charges = sum(cg.get('Amount', 0) for cg in charges if cg.get('GlAccountId') != 3)
        formatted_other_charges = f"${other_charges:,.2f}"

        total_charges = rent_charge + other_charges
        formatted_total_charges = f"${total_charges:,.2f}"
        increase = data['increasenotice'].get('increase', 0)
        formatted_increase = f"${increase:,.2f}"

        c.drawString(30,   y, tenant_names[:20])
        c.drawString(130,  y, formatted_rent_charge)
        c.drawString(200,  y, formatted_other_charges)
        c.drawString(270,  y, formatted_total_charges)
        c.drawString(350,  y, formatted_increase)
        c.drawString(430,  y, unit)
        c.drawString(510,  y, "________")

        y -= 10
        if y < 50:
            c.showPage()
            c.setFont("Helvetica", 9)
            y = 750

    # Finalize and return
    c.save()
    packet.seek(0)
    return packet
