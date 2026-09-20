import io
import re
from datetime import datetime

from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas


LONG_BOND = (8.5 * inch, 13 * inch)
LONG_BOND_LANDSCAPE = landscape(LONG_BOND)


def _text(value):
    return str(value or "").strip()


def _number(value):
    try:
        n = float(value or 0)
    except Exception:
        n = 0.0
    if abs(n - round(n)) < 1e-9:
        return str(int(round(n)))
    return f"{n:.2f}".rstrip("0").rstrip(".")


def _money(value):
    try:
        n = float(value or 0)
    except Exception:
        n = 0.0
    return f"{n:,.2f}"


def _fit_text(c, text, x, y, max_width, font="Helvetica", size=10, min_size=6):
    text = _text(text)
    if not text:
        return
    current = float(size)
    while current > min_size and c.stringWidth(text, font, current) > max_width:
        current -= 0.25
    c.setFont(font, current)
    c.drawString(x, y, text)


def _center_lines(c, lines, x, y, w, h, font="Helvetica-Bold", size=8, leading=None):
    if isinstance(lines, str):
        lines = [lines]
    lines = [str(v) for v in lines]
    if leading is None:
        leading = size + 1.5
    total_h = (len(lines) - 1) * leading
    baseline = y + (h / 2.0) + (total_h / 2.0) - (size * 0.33)
    c.setFont(font, size)
    for i, line in enumerate(lines):
        c.drawCentredString(x + w / 2.0, baseline - i * leading, line)


def _draw_underlined_field(c, label, value, x, y, label_size, line_width, value_size=10):
    c.setFont("Helvetica-Bold", label_size)
    c.drawString(x, y, label)
    label_w = c.stringWidth(label, "Helvetica-Bold", label_size)
    line_x = x + label_w + 5
    c.setLineWidth(0.8)
    c.line(line_x, y - 2, line_x + line_width, y - 2)
    if _text(value):
        _fit_text(c, value, line_x + 4, y + 1, line_width - 8, "Helvetica", value_size, 6.5)
    return line_x + line_width


def monthly_report_filename(church_name, month_name, year):
    base = f"Church Monthly Activity Record Sheet - {_text(church_name) or 'Church'} - {_text(month_name)} {year}"
    base = re.sub(r"[^A-Za-z0-9._() -]+", " ", base)
    base = re.sub(r"\s+", " ", base).strip(" .")
    return (base or "Church Monthly Activity Record Sheet") + ".pdf"


def build_monthly_activity_report_pdf(report):
    """Build the traditional Church Monthly Activity Record Sheet.

    Expected report keys:
      district_no, area_no, month_name, year, church_address, pastor_name,
      sunday_rows (list of dicts), church_progress (dict)
    """
    buffer = io.BytesIO()
    page_w, page_h = LONG_BOND_LANDSCAPE
    c = canvas.Canvas(buffer, pagesize=LONG_BOND_LANDSCAPE, pageCompression=1)
    c.setTitle("Church Monthly Activity Record Sheet")
    c.setAuthor(_text(report.get("pastor_name")) or "District 4")

    # Page margins intentionally resemble the traditional long-bond form.
    left = 38
    right = page_w - 38

    c.setStrokeColor(colors.black)
    c.setFillColor(colors.black)

    # Title
    c.setFont("Helvetica-Bold", 20)
    c.drawCentredString(page_w / 2.0, page_h - 38, "CHURCH MONTHLY ACTIVITY RECORD SHEET")

    # Header information
    header_y = page_h - 70
    x = left + 3
    x = _draw_underlined_field(c, "District No.", report.get("district_no", "4"), x, header_y, 10.5, 45, 11)
    x += 26
    x = _draw_underlined_field(c, "Area No.", report.get("area_no", ""), x, header_y, 10.5, 50, 11)
    x += 60
    x = _draw_underlined_field(c, "Month:", report.get("month_name", ""), x, header_y, 10.5, 110, 11)
    x += 28
    _draw_underlined_field(c, "Year:", report.get("year", ""), x, header_y, 10.5, 75, 11)

    address_y = header_y - 23
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(left + 3, address_y, "Church Address:")
    label_w = c.stringWidth("Church Address:", "Helvetica-Bold", 10.5)
    addr_x = left + 3 + label_w + 5
    c.line(addr_x, address_y - 2, right, address_y - 2)
    _fit_text(c, report.get("church_address", ""), addr_x + 5, address_y + 1, right - addr_x - 10, "Helvetica", 11, 7)

    # Main table geometry
    table_top = address_y - 36
    x0 = 45
    gap = 7
    left_widths = [54, 50, 50, 50, 92, 88, 88, 88]
    activity_widths = [67, 67, 67, 75]
    left_table_w = sum(left_widths)
    act_x0 = x0 + left_table_w + gap
    act_table_w = sum(activity_widths)

    header1_h = 22
    header2_h = 28
    sunday_h = 41
    total_h = 27
    body_top = table_top - header1_h - header2_h
    table_bottom = body_top - (5 * sunday_h) - total_h

    # Outer boxes
    c.setLineWidth(1.35)
    c.rect(x0, table_bottom, left_table_w, table_top - table_bottom, stroke=1, fill=0)
    c.rect(act_x0, table_bottom, act_table_w, table_top - table_bottom, stroke=1, fill=0)

    # Left section vertical lines
    xs = [x0]
    running = x0
    for w in left_widths:
        running += w
        xs.append(running)
    for xv in xs[1:-1]:
        # Attendance's internal lines begin under its group header.
        if xv in (xs[2], xs[3]):
            c.line(xv, table_bottom, xv, table_top - header1_h)
        else:
            c.line(xv, table_bottom, xv, table_top)

    # Activity section vertical lines begin below group header.
    ax = [act_x0]
    running = act_x0
    for w in activity_widths:
        running += w
        ax.append(running)
    for xv in ax[1:-1]:
        c.line(xv, table_bottom, xv, table_top - header1_h)

    # Header separator: only split grouped headers where appropriate.
    c.line(xs[1], table_top - header1_h, xs[4], table_top - header1_h)
    c.line(act_x0, table_top - header1_h, act_x0 + act_table_w, table_top - header1_h)

    # Main header/body separator.
    c.line(x0, body_top, x0 + left_table_w, body_top)
    c.line(act_x0, body_top, act_x0 + act_table_w, body_top)

    # Sunday row separators and monthly-total separator.
    for i in range(1, 6):
        y = body_top - i * sunday_h
        c.line(x0, y, x0 + left_table_w, y)
        c.line(act_x0, y, act_x0 + act_table_w, y)

    # Header labels
    _center_lines(c, "DATE", x0, table_top - header1_h - header2_h, left_widths[0], header1_h + header2_h, size=9.2)
    _center_lines(c, "ATTENDANCE", xs[1], table_top - header1_h, sum(left_widths[1:4]), header1_h, size=9.2)
    _center_lines(c, ["TOTAL", "TITHES"], xs[4], body_top, left_widths[4], header1_h + header2_h, size=8.9)
    _center_lines(c, ["TOTAL", "OFFERING"], xs[5], body_top, left_widths[5], header1_h + header2_h, size=8.9)
    _center_lines(c, ["SEED FOR", "THE GOSPEL"], xs[6], body_top, left_widths[6], header1_h + header2_h, size=8.9)
    _center_lines(c, ["PERSONAL", "TITHES"], xs[7], body_top, left_widths[7], header1_h + header2_h, size=8.9)

    for idx, label in enumerate(("ADULT", "YOUTH", "CHILDREN"), start=1):
        _center_lines(c, label, xs[idx], body_top, left_widths[idx], header2_h, size=8.5)

    _center_lines(c, "O T H E R   A C T I V I T I E S", act_x0, table_top - header1_h, act_table_w, header1_h, size=9.0)
    activity_headers = [
        ["# BIBLE", "STUDIES"],
        ["# RECEIVED", "JESUS"],
        ["# WATER", "BAPTIZED"],
        ["CHILDREN", "DEDICATION"],
    ]
    for i, lines in enumerate(activity_headers):
        _center_lines(c, lines, ax[i], body_top, activity_widths[i], header2_h, size=7.5, leading=8.4)

    # Sunday rows: exactly five slots like the paper form.
    sunday_rows = list(report.get("sunday_rows") or [])[:5]
    while len(sunday_rows) < 5:
        sunday_rows.append({})

    for i, row in enumerate(sunday_rows):
        row_y = body_top - ((i + 1) * sunday_h)
        cy = row_y + sunday_h / 2.0 - 3

        # The traditional sheet numbers the five Sunday rows just outside
        # the left edge of the table.
        c.setFont("Helvetica-Bold", 8.5)
        c.drawRightString(x0 - 8, cy, str(i + 1))
        if row:
            date_value = row.get("date")
            if isinstance(date_value, datetime):
                d = date_value.date()
            else:
                d = date_value
            if hasattr(d, "month") and hasattr(d, "day") and hasattr(d, "year"):
                date_text = f"{d.month}-{d.day}-{str(d.year)[-2:]}"
            else:
                date_text = _text(date_value)

            values = [
                date_text,
                _number(row.get("attendance_adult")),
                _number(row.get("attendance_youth")),
                _number(row.get("attendance_children")),
                _money(row.get("tithes_church")),
                _money(row.get("offering")),
                _money(row.get("mission")),
                _money(row.get("tithes_personal")),
            ]
            for col_i, value in enumerate(values):
                c.setFont("Helvetica", 9.5 if col_i < 4 else 9)
                c.drawCentredString(xs[col_i] + left_widths[col_i] / 2.0, cy, value)

        # Other Activities intentionally stay blank for individual Sundays.
        # The app stores these as monthly Church Progress values; they are
        # placed only in the MONTHLY TOTAL row below.

    # Monthly total row for Church Progress (Option B requested by user).
    total_y = table_bottom
    c.setFillColor(colors.HexColor("#F2F2F2"))
    c.rect(x0, total_y, left_table_w, total_h, stroke=0, fill=1)
    c.rect(act_x0, total_y, act_table_w, total_h, stroke=0, fill=1)
    c.setFillColor(colors.black)
    c.setLineWidth(1.35)
    c.line(x0, total_y + total_h, x0 + left_table_w, total_y + total_h)
    c.line(act_x0, total_y + total_h, act_x0 + act_table_w, total_y + total_h)
    c.line(x0, total_y, x0 + left_table_w, total_y)
    c.line(act_x0, total_y, act_x0 + act_table_w, total_y)

    # Redraw verticals through total row.
    for xv in xs[1:-1]:
        c.line(xv, total_y, xv, total_y + total_h)
    for xv in ax[1:-1]:
        c.line(xv, total_y, xv, total_y + total_h)

    _center_lines(c, ["MONTHLY", "TOTAL"], x0, total_y, left_widths[0], total_h, size=7.5, leading=8.0)

    # MONTHLY TOTAL values.
    # Attendance is averaged across the actual Sundays in the selected month.
    # Financial columns are summed across those Sundays.
    actual_sundays = [row for row in (report.get("sunday_rows") or [])[:5] if row]
    sunday_count = len(actual_sundays)

    def _sum_rows(key):
        total = 0.0
        for row in actual_sundays:
            try:
                total += float(row.get(key) or 0)
            except Exception:
                pass
        return total

    def _average_rows(key):
        if not sunday_count:
            return 0.0
        return _sum_rows(key) / sunday_count

    adult_average = _average_rows("attendance_adult")
    youth_average = _average_rows("attendance_youth")
    children_average = _average_rows("attendance_children")

    tithes_total = _sum_rows("tithes_church")
    offering_total = _sum_rows("offering")
    mission_total = _sum_rows("mission")
    personal_tithes_total = _sum_rows("tithes_personal")

    monthly_left_values = [
        None,
        _number(adult_average),
        _number(youth_average),
        _number(children_average),
        _money(tithes_total),
        _money(offering_total),
        _money(mission_total),
        _money(personal_tithes_total),
    ]

    for i in range(1, len(monthly_left_values)):
        c.setFont("Helvetica-Bold", 9.5 if i < 4 else 9.0)
        c.drawCentredString(
            xs[i] + left_widths[i] / 2.0,
            total_y + 9,
            monthly_left_values[i],
        )

    cp = dict(report.get("church_progress") or {})
    bible_total = (int(cp.get("bible_new") or 0) + int(cp.get("bible_existing") or 0))
    activity_values = [
        bible_total,
        int(cp.get("received_christ") or 0),
        int(cp.get("baptized_water") or 0),
        int(cp.get("child_dedication") or 0),
    ]
    for i, value in enumerate(activity_values):
        c.setFont("Helvetica-Bold", 10)
        c.drawCentredString(ax[i] + activity_widths[i] / 2.0, total_y + 9, str(value))

    # Monthly summary placed below the table.
    average_total_attendance = adult_average + youth_average + children_average
    # Use the same submitted Amount to Send values that the Pastor's Tool
    # writes to the Report sheet. This keeps the PDF aligned with the app's
    # official remittance calculation even if that formula changes later.
    total_amount_remitted = _sum_rows("amount_to_send")

    summary_x = left + 5
    summary_value_x = left + 165
    summary_line_w = 125

    average_y = table_bottom - 22
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(summary_x, average_y, "Average Total Attendance:")
    c.setLineWidth(0.8)
    c.line(summary_value_x, average_y - 2, summary_value_x + summary_line_w, average_y - 2)
    c.setFont("Helvetica", 10.5)
    c.drawString(summary_value_x + 5, average_y + 1, _number(average_total_attendance))

    remitted_y = average_y - 18
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(summary_x, remitted_y, "Total Amount Remitted:")
    c.line(summary_value_x, remitted_y - 2, summary_value_x + summary_line_w, remitted_y - 2)
    c.setFont("Helvetica", 10.5)
    c.drawString(summary_value_x + 5, remitted_y + 1, "Php " + _money(total_amount_remitted))

    # Testimonies/Miracles remains intentionally blank for manual writing.
    testimony_y = remitted_y - 23
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(left + 5, testimony_y, "Testimonies/Miracles:")
    test_label_w = c.stringWidth("Testimonies/Miracles:", "Helvetica-Bold", 10.5)
    c.setLineWidth(0.8)
    c.line(left + 10 + test_label_w, testimony_y - 2, right - 55, testimony_y - 2)
    c.line(left + 10 + test_label_w, testimony_y - 20, right - 55, testimony_y - 20)

    sign_y = testimony_y - 52
    # Secretary remains intentionally blank.
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(left + 100, sign_y, "Prepared by:")
    prep_line_x = left + 100 + c.stringWidth("Prepared by:", "Helvetica-Bold", 10.5) + 6
    prep_line_w = 205
    c.line(prep_line_x, sign_y - 2, prep_line_x + prep_line_w, sign_y - 2)
    c.setFont("Helvetica", 9.2)
    c.drawCentredString(prep_line_x + prep_line_w / 2.0, sign_y - 15, "Church Secretary")

    noted_x = page_w / 2.0 + 85
    c.setFont("Helvetica-Bold", 10.5)
    c.drawString(noted_x, sign_y, "Noted by:")
    noted_line_x = noted_x + c.stringWidth("Noted by:", "Helvetica-Bold", 10.5) + 6
    noted_line_w = 195
    c.line(noted_line_x, sign_y - 2, noted_line_x + noted_line_w, sign_y - 2)
    _fit_text(c, report.get("pastor_name", ""), noted_line_x + 5, sign_y + 1, noted_line_w - 10, "Helvetica", 10.5, 7.5)
    c.setFont("Helvetica", 9.2)
    c.drawCentredString(noted_line_x + noted_line_w / 2.0, sign_y - 15, "Host/Senior Pastor/Date")

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer.getvalue()
