from flask import Flask, render_template, request, redirect, url_for, Response, send_from_directory, session, has_request_context
import os
import sqlite3
import csv
import io
import re
import subprocess
import tempfile
import zipfile
import shutil
from pathlib import Path
from typing import Optional
from werkzeug.utils import secure_filename
from datetime import datetime
from collections import defaultdict

COMMON_ALLERGENS = [
    "Gluten", "Dairy", "Egg", "Soy", "Peanut", "Tree Nuts", "Fish", "Shellfish", "Sesame"
]
DIETARY_LABELS = ["Vegan", "Vegetarian"]

app = Flask(__name__)
app.secret_key = os.environ.get("KITCHEN_OPS_SECRET_KEY") or "dev-only-secret"
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_SAMESITE="Lax",
)

BASE_DIR = Path(__file__).resolve().parent
UPLOADS_DIR = BASE_DIR / "uploads"
PERSIST_DIR = Path(os.environ.get("RENDER_DISK_PATH", str(BASE_DIR)))
PERSIST_DIR.mkdir(parents=True, exist_ok=True)

INVENTORIES = {
    "uga": {"label": "UGA", "db_path": PERSIST_DIR / "kitchen_ops_uga.db"},
    "mrra": {"label": "MRRA", "db_path": PERSIST_DIR / "kitchen_ops_mrra.db"},
}
DEFAULT_INVENTORY = "uga"
LEGACY_DB_PATH = PERSIST_DIR / "kitchen_ops.db"

def ensure_inventory_dbs():
    uga_path = INVENTORIES["uga"]["db_path"]
    if LEGACY_DB_PATH.exists() and not uga_path.exists():
        shutil.copy2(LEGACY_DB_PATH, uga_path)

def current_inventory_key() -> str:
    if has_request_context():
        key = session.get("inventory_key", DEFAULT_INVENTORY)
        if key in INVENTORIES:
            return key
    return DEFAULT_INVENTORY

def current_db_path() -> Path:
    return INVENTORIES[current_inventory_key()]["db_path"]

def has_any_user() -> bool:
    conn = sqlite3.connect(INVENTORIES[DEFAULT_INVENTORY]["db_path"])
    conn.row_factory = sqlite3.Row
    try:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
        return bool(row and row["c"] > 0)
    except sqlite3.OperationalError:
        return False
    finally:
        conn.close()

def login_required(view):
    from functools import wraps

    @wraps(view)
    def wrapped(*args, **kwargs):
        if not session.get("user_id"):
            return redirect(url_for("login", next=request.path))
        return view(*args, **kwargs)

    return wrapped

def list_snapshot_files(inventory_key: str):
    prefix = f"{inventory_key}_snapshot_"
    files = []
    for p in sorted(UPLOADS_DIR.glob(f"{prefix}*.csv")):
        if p.name.endswith("_summary.csv") or p.name.endswith("_mom_units_delta.csv"):
            continue
        key = p.stem.replace(prefix, "", 1)
        label = key.replace("_", " ").title()
        files.append((key, label, p))
    return files


def get_conn():
    conn = sqlite3.connect(current_db_path())
    conn.row_factory = sqlite3.Row
    return conn


def _parse_label_list(form, list_key, custom_key):
    selected = [a.strip() for a in form.getlist(list_key) if a.strip()]
    custom = form.get(custom_key, "").strip()
    if custom:
        extras = [x.strip() for x in custom.split(",") if x.strip()]
        selected.extend(extras)

    seen = set()
    unique = []
    for a in selected:
        key = a.lower()
        if key not in seen:
            seen.add(key)
            unique.append(a)
    return ", ".join(unique)


def parse_allergens(form):
    return _parse_label_list(form, "allergens_list", "allergens_custom")


def parse_dietary(form):
    return _parse_label_list(form, "dietary_list", "dietary_custom")


def first_present(row, aliases):
    lowered = {k.lower().strip(): v for k, v in row.items()}
    for a in aliases:
        if a.lower() in lowered:
            return (lowered[a.lower()] or "").strip()
    return ""


def parse_sysco_csv_rows(data: io.StringIO):
    rows = []
    reader = csv.reader(data)
    header = None

    for raw in reader:
        if not raw:
            continue
        record_type = (raw[0] or "").strip()

        if record_type == "F":
            header = raw
            continue

        if record_type != "P" or not header:
            continue

        padded = list(raw) + [""] * max(0, len(header) - len(raw))
        row = dict(zip(header, padded))

        case_qty = (row.get("Case Qty") or "0").strip()
        split_qty = (row.get("Split Qty") or "0").strip()
        pack_size = (row.get("Pack/Size") or "").strip()
        brand = (row.get("Brand") or "").strip()
        desc = (row.get("Description") or "").strip()
        per_lb = (row.get("Per Lb") or "").strip().upper()
        case_price = (row.get("Case $") or "").strip()
        each_price = (row.get("Each $") or "").strip()

        qty_received = case_qty if case_qty not in ("", "0", "0.0") else split_qty
        unit_cost = each_price if each_price not in ("", "0", "0.0") else case_price
        item_name = " ".join(x for x in [brand, desc] if x).strip()
        if not item_name:
            item_name = desc or brand

        purchase_unit = "pound" if per_lb == "Y" else ("each" if split_qty not in ("", "0", "0.0") and case_qty in ("", "0", "0.0") else "case")

        rows.append({
            "item_name": item_name,
            "qty_received": qty_received,
            "unit_cost": unit_cost,
            "pack_size": pack_size,
            "per_lb": per_lb,
            "brand": brand,
            "description": desc,
            "purchase_unit": purchase_unit,
        })

    return rows


def infer_unit_from_text(*parts):
    text = " ".join(str(p or "") for p in parts).lower()
    normalized = re.sub(r"[^a-z0-9./# ]+", " ", text)

    if re.search(r"\b\d+\s*/\s*\d+\s*lb\b", normalized) or re.search(r"\b\d+\s*lb\b", normalized):
        return "lb"
    if re.search(r"\b\d+\s*/\s*\d+\s*oz\b", normalized) or re.search(r"\b\d+\s*oz\b", normalized):
        return "oz"
    if re.search(r"\b\d+\s*kg\b", normalized):
        return "kg"
    if re.search(r"\b\d+\s*g\b", normalized):
        return "g"
    if re.search(r"\b\d+\s*ga?l\b|\b\d+\s*gallon\b", normalized):
        return "gal"
    if re.search(r"\b\d+\s*qt\b|\bquart\b", normalized):
        return "qt"
    if re.search(r"\b\d+\s*pt\b|\bpint\b", normalized):
        return "pt"
    if re.search(r"\b\d+\s*ml\b", normalized):
        return "ml"
    if re.search(r"\b\d+\s*l\b|\bliter\b|\blitre\b", normalized):
        return "l"
    if re.search(r"\b\d+\s*ct\b|\bcount\b", normalized):
        return "ct"
    if re.search(r"\b\d+\s*dz\b|\bdoz\b|\bdozen\b", normalized):
        return "dozen"

    patterns = [
        (r"\b(case|cs)\b", "case"),
        (r"\b(pack|pk)\b", "pack"),
        (r"\b(box|bx)\b", "box"),
        (r"\b(bag|bg)\b", "bag"),
        (r"\b(bucket)\b", "bucket"),
        (r"\b(bottle|btl)\b", "bottle"),
        (r"\b(can)\b", "can"),
        (r"\b(jar)\b", "jar"),
        (r"\b(tray)\b", "tray"),
        (r"\b(tub)\b", "tub"),
        (r"\b(lb|lbs|pound|pounds)\b", "lb"),
        (r"\b(oz|ounce|ounces)\b", "oz"),
        (r"\b(kg|kilogram|kilograms)\b", "kg"),
        (r"\b(g|gram|grams)\b", "g"),
        (r"\b(gal|gallon|gallons)\b", "gal"),
        (r"\b(qt|quart|quarts)\b", "qt"),
        (r"\b(pt|pint|pints)\b", "pt"),
        (r"\b(l|liter|liters|litre|litres)\b", "l"),
        (r"\b(ml|milliliter|milliliters|millilitre|millilitres)\b", "ml"),
        (r"\b(doz|dozen)\b", "dozen"),
        (r"\b(each|ea)\b", "ea"),
    ]

    for pattern, unit in patterns:
        if re.search(pattern, normalized):
            return unit

    if re.search(r"\b\d+\s*/\s*\d+\b", normalized):
        return "case"

    return "ea"


def ocr_pdf_text(pdf_path: Path) -> str:
    swift_code = f'''
import Foundation
import PDFKit
import Vision
import AppKit

let path = "{str(pdf_path)}"
guard let doc = PDFDocument(url: URL(fileURLWithPath: path)) else {{
    fputs("Failed to open PDF\\n", stderr)
    exit(1)
}}

for i in 0..<doc.pageCount {{
    guard let page = doc.page(at: i) else {{ continue }}
    let bounds = page.bounds(for: .mediaBox)
    let scale: CGFloat = 2.2
    let width = Int(bounds.width * scale)
    let height = Int(bounds.height * scale)

    guard let colorSpace = CGColorSpace(name: CGColorSpace.sRGB),
          let ctx = CGContext(data: nil, width: width, height: height, bitsPerComponent: 8, bytesPerRow: 0, space: colorSpace, bitmapInfo: CGImageAlphaInfo.noneSkipLast.rawValue) else {{
        continue
    }}
    ctx.setFillColor(NSColor.white.cgColor)
    ctx.fill(CGRect(x: 0, y: 0, width: CGFloat(width), height: CGFloat(height)))
    ctx.saveGState()
    ctx.scaleBy(x: scale, y: scale)
    page.draw(with: .mediaBox, to: ctx)
    ctx.restoreGState()

    guard let cg = ctx.makeImage() else {{ continue }}

    let req = VNRecognizeTextRequest()
    req.recognitionLevel = .accurate
    req.usesLanguageCorrection = false
    req.recognitionLanguages = ["en-US"]

    let handler = VNImageRequestHandler(cgImage: cg, options: [:])
    try? handler.perform([req])

    print("===== PAGE \\(i+1) =====")
    for o in req.results ?? [] {{
        if let best = o.topCandidates(1).first {{
            print(best.string)
        }}
    }}
}}
'''
    with tempfile.NamedTemporaryFile(mode="w", suffix=".swift", delete=False) as sf:
        sf.write(swift_code)
        swift_file = sf.name

    try:
        res = subprocess.run(["swift", swift_file], capture_output=True, text=True, timeout=180)
        return (res.stdout or "")
    finally:
        try:
            Path(swift_file).unlink(missing_ok=True)
        except Exception:
            pass


def parse_royal_ocr(text: str):
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    out = []

    money_re = re.compile(r"^\$?\d+(?:\.\d{2,3})?$")
    code_re = re.compile(r"^\d{4,6}$")
    pack_re = re.compile(r"\b\d+(?:/\d+)?\s*(lb|lbs|oz|kg|g|ct|dz|gal|ga|qt|pt|ml|l|cs|case|bag|bx|pk|each|ea)\b", re.I)
    unit_words = {"case", "pound", "bag", "each", "dozen", "box", "pack"}
    stop_words = {"invoice", "description", "quantity", "ordered", "shipped", "special instructions", "subtotal", "tax", "total", "page", "customer"}
    blocked_phrases = {
        "order qty", "qty received", "unit cost", "item code", "zip", "industrial blvd", "atlanta", "georgia",
        "royal food service", "food service", "inc.", "invoice date", "customer #", "invoice #"
    }

    def maybe_desc(s: str) -> bool:
        sl = s.lower().strip()
        if not sl or len(sl) < 3:
            return False
        if sl in unit_words:
            return False
        if code_re.fullmatch(sl):
            return False
        if money_re.fullmatch(sl.replace(",", "")):
            return False
        if any(word == sl for word in stop_words):
            return False
        if any(phrase in sl for phrase in blocked_phrases):
            return False
        if len(re.findall(r"\d", sl)) > 2:
            return False
        return bool(re.search(r"[a-z]", sl))

    code_indexes = [idx for idx, ln in enumerate(lines) if code_re.fullmatch(ln)]

    for pos, idx in enumerate(code_indexes):
        item_code = lines[idx]
        next_idx = code_indexes[pos + 1] if pos + 1 < len(code_indexes) else len(lines)
        block_before = lines[max(0, idx - 3):idx]
        block_after = lines[idx + 1:min(next_idx, idx + 10)]

        desc_candidates = [x for x in block_before if maybe_desc(x)]
        if not desc_candidates:
            desc_candidates = [x for x in block_after[:3] if maybe_desc(x)]
        item_name = " ".join(desc_candidates[-2:]).strip()
        if not item_name:
            continue
        if any(phrase in item_name.lower() for phrase in blocked_phrases):
            continue

        pack_size = ""
        purchase_unit = ""
        prices = []

        for w in block_before + block_after:
            wl = w.lower().strip()
            if not pack_size and pack_re.search(wl):
                pack_size = w
                continue
            if not purchase_unit and wl in unit_words:
                purchase_unit = wl
                continue
            cleaned = w.replace("$", "").replace(",", "")
            if money_re.fullmatch(cleaned):
                prices.append(cleaned)

        unit_cost = prices[-1] if prices else ""

        out.append({
            "item_name": item_name,
            "qty_received": 1,
            "unit_cost": unit_cost,
            "pack_size": pack_size,
            "purchase_unit": purchase_unit or "case",
            "item_code": item_code,
        })

    deduped = []
    seen = set()
    for row in out:
        key = (row["item_name"], row.get("pack_size"), row.get("unit_cost"), row.get("item_code"))
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    return deduped


def parse_sysco_ocr(text: str):
    out = []
    for ln in text.splitlines():
        s = ln.strip()
        m = re.match(r"^(\d+(?:\.\d+)?)\s+(CS|BG|EA|LB|GAL|OZ|SCS|S)\s+(.+)$", s, re.I)
        if not m:
            continue
        qty = float(m.group(1))
        desc = m.group(3)
        desc = re.sub(r"\b\d{5,}[\w-]*\b.*$", "", desc).strip()
        if len(desc) < 4:
            continue
        out.append({"item_name": desc, "qty_received": qty, "unit_cost": None})
    return out


def parse_invoice_pdf(pdf_path: Path):
    text = ocr_pdf_text(pdf_path)
    vendor = "unknown"
    rows = []
    if "royal food service" in text.lower():
        vendor = "royal"
        rows = parse_royal_ocr(text)
    elif "sysco" in text.lower():
        vendor = "sysco"
        rows = parse_sysco_ocr(text)
    return vendor, rows, text


def init_db(db_path: Path | None = None):
    conn = sqlite3.connect(db_path or current_db_path())
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            unit TEXT NOT NULL,
            purchase_unit TEXT,
            pack_size TEXT,
            vendor TEXT,
            cost_per_unit REAL NOT NULL DEFAULT 0,
            on_hand REAL NOT NULL DEFAULT 0,
            reorder_level REAL NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            servings REAL NOT NULL DEFAULT 1,
            sale_price REAL NOT NULL DEFAULT 0,
            image_path TEXT,
            allergens TEXT,
            dietary_labels TEXT
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS recipe_ingredients (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            recipe_id INTEGER NOT NULL,
            item_id INTEGER NOT NULL,
            quantity REAL NOT NULL DEFAULT 0,
            display_quantity REAL,
            display_unit TEXT,
            conversion_factor REAL NOT NULL DEFAULT 1,
            FOREIGN KEY(recipe_id) REFERENCES recipes(id),
            FOREIGN KEY(item_id) REFERENCES items(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS waste_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_id INTEGER NOT NULL,
            date TEXT NOT NULL,
            qty REAL NOT NULL,
            unit TEXT,
            cost_per_unit_at_time REAL NOT NULL,
            reason TEXT,
            total_value REAL NOT NULL,
            FOREIGN KEY(item_id) REFERENCES items(id)
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_transfers (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            transfer_date TEXT NOT NULL,
            item_id INTEGER NOT NULL,
            qty REAL NOT NULL,
            display_qty REAL,
            display_unit TEXT,
            conversion_factor REAL NOT NULL DEFAULT 1,
            transfer_from TEXT,
            transfer_to TEXT,
            note TEXT,
            FOREIGN KEY(item_id) REFERENCES items(id)
        )
        """
    )
    
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            purchase_date DATE NOT NULL,
            company VARCHAR(100) NOT NULL,
            invoice_number VARCHAR(50),
            invoice_total DECIMAL(10, 2) NOT NULL,
            po_number VARCHAR(100)
        )
        """
    )


    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_budgets (
            month TEXT PRIMARY KEY,
            budget DECIMAL(10, 2) NOT NULL DEFAULT 0
        )
        """
    )


    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS inventory_snapshots (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            snapshot_date TEXT NOT NULL,
            snapshot_month TEXT NOT NULL,
            item_name TEXT NOT NULL,
            quantity REAL NOT NULL,
            unit TEXT,
            cost_per_unit REAL NOT NULL,
            total_value REAL NOT NULL
        )
        """
    )


    # Lightweight migrations for older DBs.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS vendors (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            rep_name TEXT,
            email TEXT,
            phone TEXT,
            notes TEXT
        )
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO vendors (name)
        SELECT DISTINCT vendor FROM items WHERE vendor IS NOT NULL AND vendor != ''
        """
    )
    
    recipe_cols = {row[1] for row in conn.execute("PRAGMA table_info(recipes)").fetchall()}
    if "sale_price" not in recipe_cols:
        conn.execute("ALTER TABLE recipes ADD COLUMN sale_price REAL NOT NULL DEFAULT 0")
    if "image_path" not in recipe_cols:
        conn.execute("ALTER TABLE recipes ADD COLUMN image_path TEXT")
    if "allergens" not in recipe_cols:
        conn.execute("ALTER TABLE recipes ADD COLUMN allergens TEXT")
    if "dietary_labels" not in recipe_cols:
        conn.execute("ALTER TABLE recipes ADD COLUMN dietary_labels TEXT")

    item_cols = {row[1] for row in conn.execute("PRAGMA table_info(items)").fetchall()}
    if "purchase_unit" not in item_cols:
        conn.execute("ALTER TABLE items ADD COLUMN purchase_unit TEXT")
    if "pack_size" not in item_cols:
        conn.execute("ALTER TABLE items ADD COLUMN pack_size TEXT")

    transfer_cols = {row[1] for row in conn.execute("PRAGMA table_info(inventory_transfers)").fetchall()}
    if "display_qty" not in transfer_cols:
        conn.execute("ALTER TABLE inventory_transfers ADD COLUMN display_qty REAL")
    if "display_unit" not in transfer_cols:
        conn.execute("ALTER TABLE inventory_transfers ADD COLUMN display_unit TEXT")
    if "conversion_factor" not in transfer_cols:
        conn.execute("ALTER TABLE inventory_transfers ADD COLUMN conversion_factor REAL NOT NULL DEFAULT 1")
    conn.execute(
        """
        UPDATE inventory_transfers
        SET display_qty = COALESCE(display_qty, qty),
            conversion_factor = COALESCE(conversion_factor, 1)
        WHERE display_qty IS NULL OR conversion_factor IS NULL
        """
    )

    cols = {row[1] for row in conn.execute("PRAGMA table_info(recipe_ingredients)").fetchall()}
    if "display_quantity" not in cols:
        conn.execute("ALTER TABLE recipe_ingredients ADD COLUMN display_quantity REAL")
    if "display_unit" not in cols:
        conn.execute("ALTER TABLE recipe_ingredients ADD COLUMN display_unit TEXT")
    if "conversion_factor" not in cols:
        conn.execute("ALTER TABLE recipe_ingredients ADD COLUMN conversion_factor REAL NOT NULL DEFAULT 1")

    conn.execute(
        """
        UPDATE recipe_ingredients
        SET display_quantity = COALESCE(display_quantity, quantity),
            conversion_factor = COALESCE(conversion_factor, 1)
        WHERE display_quantity IS NULL OR conversion_factor IS NULL
        """
    )

    UPLOADS_DIR.mkdir(parents=True, exist_ok=True)
    conn.commit()
    conn.close()

@app.route("/login", methods=["GET", "POST"])
def login():
    conn = sqlite3.connect(INVENTORIES[DEFAULT_INVENTORY]["db_path"])
    conn.row_factory = sqlite3.Row

    if request.method == "POST":
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""

        row = conn.execute(
            "SELECT id, email, password_hash FROM users WHERE lower(email)=?",
            (email,),
        ).fetchone()

        if row and check_password_hash(row["password_hash"], password):
            session["user_id"] = row["id"]
            session["user_email"] = row["email"]
            conn.close()
            return redirect(url_for("dashboard"))

        conn.close()
        return render_template("login.html", error="Invalid email or password")

    conn.close()
    return render_template("login.html")

@app.context_processor
def inject_inventory_context():
    key = current_inventory_key()
    return {
        "active_inventory_key": key,
        "active_inventory_label": INVENTORIES[key]["label"],
        "inventory_options": [(k, v["label"]) for k, v in INVENTORIES.items()],
    }


@app.route("/inventory/select/<inventory_key>")
@login_required
def select_inventory(inventory_key):
    if inventory_key in INVENTORIES:
        session["inventory_key"] = inventory_key
    return redirect(request.referrer or url_for("dashboard"))


@app.route("/")
@login_required
def dashboard():
    conn = get_conn()
    items = conn.execute("SELECT * FROM items ORDER BY name").fetchall()
    recipe_count = conn.execute("SELECT COUNT(*) AS c FROM recipes").fetchone()["c"]
    conn.close()

    low_stock = [i for i in items if i["on_hand"] <= i["reorder_level"]]
    total_value = sum(i["on_hand"] * i["cost_per_unit"] for i in items)

    item_count = len(items)
    first_run = item_count == 0 and recipe_count == 0

    return render_template(
        "dashboard.html",
        item_count=item_count,
        recipe_count=recipe_count,
        low_stock_count=len(low_stock),
        total_value=total_value,
        first_run=first_run,
    )


@app.route("/setup")
def setup_wizard():
    if has_any_user() and not session.get("user_id"):
        return redirect(url_for("setup_wizard"))
    conn = get_conn()
    item_count = conn.execute("SELECT COUNT(*) AS c FROM items").fetchone()["c"]
    recipe_count = conn.execute("SELECT COUNT(*) AS c FROM recipes").fetchone()["c"]
    conn.close()

    checklist = [
        {
            "title": "Import inventory",
            "done": item_count > 0,
            "hint": "Upload invoices or import a vendor CSV to build your inventory baseline.",
            "href": url_for("inventory_upload_invoice"),
            "cta": "Upload Invoice",
        },
        {
            "title": "Review inventory",
            "done": item_count > 0,
            "hint": "Verify units, vendors, costs, and on-hand counts.",
            "href": url_for("inventory"),
            "cta": "Open Inventory",
        },
        {
            "title": "Build recipes",
            "done": recipe_count > 0,
            "hint": "Create recipes from inventory items so menu planning can scale automatically.",
            "href": url_for("recipes"),
            "cta": "Open Recipes",
        },
        {
            "title": "Build menu + generate vendor order",
            "done": False,
            "hint": "Use Menu Builder to scale recipes and export vendor-specific POs.",
            "href": url_for("order_from_menu"),
            "cta": "Open Menu Builder",
        },
    ]

    return render_template(
        "setup.html",
        item_count=item_count,
        recipe_count=recipe_count,
        checklist=checklist,
    )


@app.route("/uploads/<path:filename>")
@login_required
def uploads(filename):
    return send_from_directory(UPLOADS_DIR, filename)


@app.route("/inventory")
@login_required
def inventory():
    q = (request.args.get("q") or "").strip()
    vendor = (request.args.get("vendor") or "").strip()
    month = (request.args.get("month") or "current").strip().lower()

    inv_key = current_inventory_key()
    snapshot_files = list_snapshot_files(inv_key)
    snapshot_map = {k: p for k, _label, p in snapshot_files}
    month_options = [("current", "Current (Editable)")] + [(k, lbl) for k, lbl, _p in snapshot_files]

    readonly_mode = month != "current"
    items = []
    vendors = []

    if readonly_mode and month in snapshot_map:
        with open(snapshot_map[month], newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                name = (row.get("item_name") or "").strip()
                unit = (row.get("count_unit") or "ea").strip()
                if not name:
                    continue
                cost = float(row.get("unit_price") or 0)
                on_hand = float(row.get("total_units") or 0)
                item_vendor = INVENTORIES[inv_key]["label"]
                item = {
                    "id": None,
                    "name": name,
                    "unit": unit,
                    "vendor": item_vendor,
                    "cost_per_unit": cost,
                    "on_hand": on_hand,
                    "reorder_level": 0,
                }
                items.append(item)
        vendors = sorted({i["vendor"] for i in items if i["vendor"]})
    else:
        month = "current"
        readonly_mode = False
        conn = get_conn()
        vendor_rows = conn.execute(
            "SELECT DISTINCT vendor FROM items WHERE coalesce(vendor,'') <> '' ORDER BY vendor"
        ).fetchall()

        sql = "SELECT * FROM items WHERE 1=1"
        params = []
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        if vendor:
            sql += " AND vendor = ?"
            params.append(vendor)
        sql += " ORDER BY name"

        items = conn.execute(sql, params).fetchall()
        conn.close()
        vendors = [r["vendor"] for r in vendor_rows]

    if q:
        items = [i for i in items if q.lower() in i["name"].lower()]
    if vendor:
        items = [i for i in items if (i["vendor"] or "") == vendor]

    normalized_items = []
    for i in items:
        item = dict(i)
        item["item_value"] = float(item.get("on_hand") or 0) * float(item.get("cost_per_unit") or 0)
        normalized_items.append(item)
    items = normalized_items

    low_stock = [i for i in items if i["on_hand"] <= i["reorder_level"]]
    total_value = sum(i["item_value"] for i in items)

    return render_template(
        "inventory.html",
        items=items,
        low_stock=low_stock,
        total_value=total_value,
        q=q,
        vendor=vendor,
        vendors=vendors,
        month=month,
        month_options=month_options,
        readonly_mode=readonly_mode,
    )


@app.route("/inventory/count-sheet.csv")
@login_required
def inventory_count_sheet_csv():
    conn = get_conn()
    items = conn.execute("SELECT id, name, unit, on_hand, vendor FROM items ORDER BY name").fetchall()
    conn.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "item_id", "item_name", "unit", "vendor", "current_on_hand", "counted_qty", "notes"
    ])
    for i in items:
        writer.writerow([i["id"], i["name"], i["unit"], i["vendor"] or "", i["on_hand"], "", ""])

    stamp = datetime.now().strftime("%Y%m%d")
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename=inventory_count_sheet_{stamp}.csv"},
    )


@app.route("/inventory/upload-counts", methods=["GET", "POST"])
@login_required
def inventory_upload_counts():
    if request.method == "POST":
        f = request.files.get("file")
        if not f or not f.filename:
            return render_template(
                "upload_counts.html",
                message="Please choose a CSV file.",
                applied=0,
                exception_file=None,
                variance_file=None,
                exception_count=0,
                variance_count=0,
            )

        data = io.StringIO(f.stream.read().decode("utf-8-sig"))
        reader = csv.DictReader(data)

        conn = get_conn()
        applied = 0
        exceptions = []
        variance_rows = []

        for idx, row in enumerate(reader, start=2):
            raw_count = (row.get("counted_qty") or "").strip()
            item_id = (row.get("item_id") or "").strip()
            item_name = (row.get("item_name") or "").strip()
            current_on_hand_raw = (row.get("current_on_hand") or "").strip() or "0"

            if raw_count == "":
                exceptions.append([idx, item_id, item_name, "Blank counted_qty"])
                continue
            try:
                counted = float(raw_count)
            except ValueError:
                exceptions.append([idx, item_id, item_name, f"Invalid counted_qty: {raw_count}"])
                continue

            try:
                sheet_on_hand = float(current_on_hand_raw)
            except ValueError:
                sheet_on_hand = 0.0

            target = None
            if item_id.isdigit():
                target = conn.execute("SELECT id, name, on_hand FROM items WHERE id = ?", (int(item_id),)).fetchone()
            elif item_name:
                target = conn.execute("SELECT id, name, on_hand FROM items WHERE name = ?", (item_name,)).fetchone()
            else:
                exceptions.append([idx, item_id, item_name, "Missing item_id and item_name"])
                continue

            if not target:
                exceptions.append([idx, item_id, item_name, "Item not found"])
                continue

            previous_on_hand = float(target["on_hand"])
            variance = counted - sheet_on_hand

            cur = conn.execute("UPDATE items SET on_hand = ? WHERE id = ?", (counted, target["id"]))
            if cur.rowcount and cur.rowcount > 0:
                applied += 1
                variance_rows.append([
                    target["id"], target["name"], sheet_on_hand, counted, variance, previous_on_hand
                ])
            else:
                exceptions.append([idx, item_id, item_name, "No row updated"])

        conn.commit()
        conn.close()

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        exception_filename = f"inventory_exceptions_{stamp}.csv"
        variance_filename = f"inventory_variance_{stamp}.csv"

        with open(UPLOADS_DIR / exception_filename, "w", newline="", encoding="utf-8") as ef:
            w = csv.writer(ef)
            w.writerow(["line", "item_id", "item_name", "reason"])
            w.writerows(exceptions)

        with open(UPLOADS_DIR / variance_filename, "w", newline="", encoding="utf-8") as vf:
            w = csv.writer(vf)
            w.writerow(["item_id", "item_name", "sheet_on_hand", "counted_qty", "variance_vs_sheet", "db_on_hand_before_upload"])
            w.writerows(variance_rows)

        return render_template(
            "upload_counts.html",
            message="Inventory counts uploaded.",
            applied=applied,
            exception_file=exception_filename,
            variance_file=variance_filename,
            exception_count=len(exceptions),
            variance_count=len(variance_rows),
        )

    return render_template("upload_counts.html", message=None, applied=0, exception_file=None, variance_file=None, exception_count=0, variance_count=0)


@app.route("/inventory/upload-invoice", methods=["GET", "POST"])
@login_required
def inventory_upload_invoice():
    if request.method == "POST":
        action = request.form.get("action", "parse")

        if action == "apply":
            token = request.form.get("review_token", "")
            review_path = UPLOADS_DIR / token
            if not token or not review_path.exists():
                return render_template("upload_invoice.html", message="Review session expired. Re-upload invoice.", applied=0, exception_file=None, vendor="")

            data = io.StringIO(review_path.read_text(encoding="utf-8"))
            parsed_rows = list(csv.DictReader(data))
            vendor = request.form.get("vendor", "csv")

            conn = get_conn()
            inv_items = conn.execute("SELECT id, name, unit, on_hand, cost_per_unit FROM items").fetchall()
            applied = 0
            created = 0
            exceptions = []

            def find_item(name):
                n = (name or "").strip().lower()
                if not n:
                    return None
                for it in inv_items:
                    if it["name"].strip().lower() == n:
                        return it
                for it in inv_items:
                    iname = it["name"].strip().lower()
                    if n in iname or iname in n:
                        return it
                return None

            for idx, row in enumerate(parsed_rows, start=2):
                item_name = (row.get("item_name") or "").strip()
                raw_qty = str(row.get("qty_received") or "").strip()
                raw_cost = str(row.get("unit_cost") or "").strip()

                if not item_name:
                    exceptions.append([idx, "", "Missing item name"])
                    continue
                if not raw_qty:
                    exceptions.append([idx, item_name, "Missing qty_received"])
                    continue

                try:
                    qty = float(raw_qty)
                except ValueError:
                    exceptions.append([idx, item_name, f"Invalid qty_received: {raw_qty}"])
                    continue

                parsed_cost = 0.0
                if raw_cost:
                    try:
                        parsed_cost = float(raw_cost.replace("$", ""))
                    except ValueError:
                        exceptions.append([idx, item_name, f"Invalid unit_cost: {raw_cost}"])
                        continue

                item = find_item(item_name)
                if not item:
                    inferred_unit = infer_unit_from_text(
                        item_name,
                        row.get("pack_size"),
                        row.get("description"),
                        raw_qty,
                        raw_cost,
                        vendor,
                    )
                    conn.execute(
                        "INSERT INTO items (name, unit, purchase_unit, pack_size, vendor, cost_per_unit, on_hand, reorder_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                        (
                            item_name,
                            inferred_unit,
                            row.get("purchase_unit"),
                            row.get("pack_size"),
                            vendor,
                            parsed_cost,
                            qty,
                            0,
                        ),
                    )
                    created += 1
                    applied += 1
                    new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
                    inv_items = conn.execute("SELECT id, name, unit, on_hand, cost_per_unit FROM items").fetchall()
                    item = conn.execute("SELECT id, name, unit, on_hand, cost_per_unit FROM items WHERE id = ?", (new_id,)).fetchone()
                    continue

                new_on_hand = float(item["on_hand"]) + qty
                new_cost = float(item["cost_per_unit"])
                if raw_cost:
                    new_cost = parsed_cost

                conn.execute(
                    "UPDATE items SET on_hand = ?, cost_per_unit = ? WHERE id = ?",
                    (new_on_hand, new_cost, item["id"]),
                )
                applied += 1

            conn.commit()
            conn.close()

            stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            exception_file = f"invoice_upload_exceptions_{stamp}.csv"
            with open(UPLOADS_DIR / exception_file, "w", newline="", encoding="utf-8") as ef:
                w = csv.writer(ef)
                w.writerow(["line", "item_name", "reason"])
                w.writerows(exceptions)

            review_path.unlink(missing_ok=True)
            return render_template(
                "upload_invoice.html",
                message=f"Invoice upload processed ({vendor}). Updated/added {applied} item(s), including {created} new item(s).",
                applied=applied,
                exception_file=exception_file,
                vendor=vendor,
            )

        # parse phase
        f = request.files.get("file")
        if not f or not f.filename:
            return render_template("upload_invoice.html", message="Please choose an invoice file.", applied=0, exception_file=None, vendor="")

        filename = f.filename.lower()
        parsed_rows = []
        vendor = "csv"

        if filename.endswith(".csv"):
            raw_csv = f.stream.read().decode("utf-8-sig")
            if raw_csv.lstrip().startswith("H,") or "\nF,SUPC," in raw_csv:
                vendor = "sysco"
                parsed_rows = parse_sysco_csv_rows(io.StringIO(raw_csv))
            else:
                data = io.StringIO(raw_csv)
                reader = csv.DictReader(data)
                for row in reader:
                    parsed_rows.append({
                        "item_name": first_present(row, ["item_name", "item", "description", "product"]),
                        "qty_received": first_present(row, ["qty_received", "received_qty", "qty", "quantity"]),
                        "unit_cost": first_present(row, ["unit_cost", "cost_per_unit", "unit_price", "price"]),
                    })
        elif filename.endswith(".pdf"):
            tmp_path = UPLOADS_DIR / f"tmp_invoice_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
            f.save(tmp_path)
            try:
                vendor, pdf_rows, _ = parse_invoice_pdf(tmp_path)
                parsed_rows = pdf_rows
            finally:
                tmp_path.unlink(missing_ok=True)

        if not parsed_rows:
            return render_template(
                "upload_invoice.html",
                message="No invoice rows were parsed. Try CSV or a clearer PDF.",
                applied=0,
                exception_file=None,
                vendor=vendor,
            )

        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        token = f"invoice_review_{stamp}.csv"
        review_path = UPLOADS_DIR / token
        with open(review_path, "w", newline="", encoding="utf-8") as rf:
            w = csv.writer(rf)
            w.writerow(["item_name", "qty_received", "unit_cost"])
            for r in parsed_rows:
                w.writerow([r.get("item_name", ""), r.get("qty_received", ""), r.get("unit_cost", "")])

        return render_template(
            "upload_invoice.html",
            message="Review parsed rows, then click Apply Invoice.",
            applied=0,
            exception_file=None,
            vendor=vendor,
            review_rows=parsed_rows,
            review_token=token,
        )

    return render_template("upload_invoice.html", message=None, applied=0, exception_file=None, vendor="", review_rows=None, review_token=None)


@app.route("/vendors", methods=["GET", "POST"])
@login_required
def vendors():
    conn = get_conn()
    if request.method == "POST":
        action = request.form.get("action", "add")
        if action == "add":
            name = request.form["name"].strip()
            rep_name = request.form.get("rep_name", "").strip()
            email = request.form.get("email", "").strip()
            phone = request.form.get("phone", "").strip()
            notes = request.form.get("notes", "").strip()
            try:
                conn.execute(
                    "INSERT INTO vendors (name, rep_name, email, phone, notes) VALUES (?, ?, ?, ?, ?)",
                    (name, rep_name, email, phone, notes)
                )
                conn.commit()
            except sqlite3.IntegrityError:
                pass
        elif action == "edit":
            vid = int(request.form["vendor_id"])
            name = request.form["name"].strip()
            rep_name = request.form.get("rep_name", "").strip()
            email = request.form.get("email", "").strip()
            phone = request.form.get("phone", "").strip()
            notes = request.form.get("notes", "").strip()
            
            old_name_row = conn.execute("SELECT name FROM vendors WHERE id = ?", (vid,)).fetchone()
            if old_name_row and old_name_row["name"] != name:
                conn.execute("UPDATE items SET vendor = ? WHERE vendor = ?", (name, old_name_row["name"]))
                
            conn.execute(
                "UPDATE vendors SET name = ?, rep_name = ?, email = ?, phone = ?, notes = ? WHERE id = ?",
                (name, rep_name, email, phone, notes, vid)
            )
            conn.commit()
        elif action == "delete":
            vid = int(request.form["vendor_id"])
            conn.execute("DELETE FROM vendors WHERE id = ?", (vid,))
            conn.commit()

    vendors_list = conn.execute("SELECT * FROM vendors ORDER BY name").fetchall()
    conn.close()
    return render_template("vendors.html", vendors=vendors_list)

@app.route("/items/new", methods=["GET", "POST"])
@login_required
def new_item():
    if request.method == "POST":
        name = request.form["name"].strip()
        unit = request.form["unit"].strip()
        purchase_unit = request.form.get("purchase_unit", "").strip()
        pack_size = request.form.get("pack_size", "").strip()
        vendor = request.form.get("vendor", "").strip()
        cost_per_unit = float(request.form.get("cost_per_unit", 0) or 0)
        on_hand = float(request.form.get("on_hand", 0) or 0)
        reorder_level = float(request.form.get("reorder_level", 0) or 0)

        conn = get_conn()
        conn.execute(
            "INSERT INTO items (name, unit, purchase_unit, pack_size, vendor, cost_per_unit, on_hand, reorder_level) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (name, unit, purchase_unit, pack_size, vendor, cost_per_unit, on_hand, reorder_level),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("dashboard"))

    return render_template("new_item.html")


@app.route("/items/<int:item_id>/edit", methods=["GET", "POST"])
@login_required
def edit_item(item_id):
    conn = get_conn()
    item = conn.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
    if not item:
        conn.close()
        return redirect(url_for("inventory"))

    if request.method == "POST":
        name = request.form["name"].strip()
        unit = request.form["unit"].strip()
        purchase_unit = request.form.get("purchase_unit", "").strip()
        pack_size = request.form.get("pack_size", "").strip()
        vendor = request.form.get("vendor", "").strip()
        cost_per_unit = float(request.form.get("cost_per_unit", 0) or 0)
        on_hand = float(request.form.get("on_hand", 0) or 0)
        reorder_level = float(request.form.get("reorder_level", 0) or 0)

        conn.execute(
            """
            UPDATE items
            SET name = ?, unit = ?, purchase_unit = ?, pack_size = ?, vendor = ?, cost_per_unit = ?, on_hand = ?, reorder_level = ?
            WHERE id = ?
            """,
            (name, unit, purchase_unit, pack_size, vendor, cost_per_unit, on_hand, reorder_level, item_id),
        )
        conn.commit()
        conn.close()
        return redirect(url_for("inventory"))

    conn.close()
    return render_template("edit_item.html", item=item)


@app.route("/items/<int:item_id>/delete", methods=["POST"])
@login_required
def delete_item(item_id):
    conn = get_conn()
    conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("inventory"))


def build_suggested_rows():
    conn = get_conn()
    items = conn.execute("SELECT * FROM items ORDER BY name").fetchall()
    conn.close()

    suggested = []
    for i in items:
        need = i["reorder_level"] - i["on_hand"]
        if need > 0:
            suggested.append({
                "name": i["name"],
                "unit": i["unit"],
                "vendor": i["vendor"],
                "order_qty": round(need, 2),
                "est_cost": round(need * i["cost_per_unit"], 2),
            })
    return suggested


@app.route("/orders/suggested")
@login_required
def suggested_order():
    suggested = build_suggested_rows()
    return render_template("suggested_order.html", suggested=suggested)


@app.route("/inventory/transfers", methods=["GET", "POST"])
@login_required
def inventory_transfers():
    month = (request.args.get("month") or datetime.now().strftime("%Y-%m"))

    conn = get_conn()
    if request.method == "POST":
        action = request.form.get("action", "add")

        if action == "add":
            transfer_date = request.form.get("transfer_date") or datetime.now().strftime("%Y-%m-%d")
            item_id = int(request.form["item_id"])
            display_qty = float(request.form.get("qty", 0) or 0)
            display_unit = request.form.get("display_unit", "").strip()
            conversion_factor = float(request.form.get("conversion_factor", 1) or 1)
            qty = display_qty * conversion_factor
            transfer_from = request.form.get("transfer_from", "").strip()
            transfer_to = request.form.get("transfer_to", "").strip()
            note = request.form.get("note", "").strip()

            conn.execute(
                """
                INSERT INTO inventory_transfers
                (transfer_date, item_id, qty, display_qty, display_unit, conversion_factor, transfer_from, transfer_to, note)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (transfer_date, item_id, qty, display_qty, display_unit, conversion_factor, transfer_from, transfer_to, note),
            )
            
            # --- NEW TRANSFER LOGIC ---
            # 1. Deduct from current inventory
            conn.execute("UPDATE items SET on_hand = on_hand - ? WHERE id = ?", (qty, item_id))
            conn.commit()
            
            # 2. Add to target inventory if transfer_to matches another dashboard
            current_key = current_inventory_key()
            target_key = None
            t_to_lower = transfer_to.strip().lower()
            for k, v in INVENTORIES.items():
                if t_to_lower == k.lower() or t_to_lower == v["label"].lower():
                    target_key = k
                    break
            
            if target_key and target_key != current_key:
                # Fetch item details to match or create in target DB
                item = conn.execute("SELECT name, unit, vendor, cost_per_unit FROM items WHERE id = ?", (item_id,)).fetchone()
                if item:
                    target_db_path = INVENTORIES[target_key]["db_path"]
                    import sqlite3
                    t_conn = sqlite3.connect(target_db_path)
                    t_conn.row_factory = sqlite3.Row
                    
                    # Find by name
                    t_item = t_conn.execute("SELECT id FROM items WHERE lower(name) = ?", (item["name"].lower(),)).fetchone()
                    if t_item:
                        t_conn.execute("UPDATE items SET on_hand = on_hand + ? WHERE id = ?", (qty, t_item["id"]))
                        t_item_id = t_item["id"]
                    else:
                        cur = t_conn.execute(
                            "INSERT INTO items (name, unit, vendor, cost_per_unit, on_hand, reorder_level) VALUES (?, ?, ?, ?, ?, 0)",
                            (item["name"], item["unit"], item["vendor"], item["cost_per_unit"], qty)
                        )
                        t_item_id = cur.lastrowid
                        
                    # Also log the transfer in the target DB so they have a record of it arriving!
                    t_conn.execute(
                        """
                        INSERT INTO inventory_transfers
                        (transfer_date, item_id, qty, display_qty, display_unit, conversion_factor, transfer_from, transfer_to, note)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (transfer_date, t_item_id, qty, display_qty, display_unit, conversion_factor, transfer_from, transfer_to, note),
                    )
                    t_conn.commit()
                    t_conn.close()
            # --------------------------
            
            month = transfer_date[:7]

        elif action == "update":
            transfer_id = int(request.form["transfer_id"])
            transfer_date = request.form.get("transfer_date") or datetime.now().strftime("%Y-%m-%d")
            display_qty = float(request.form.get("qty", 0) or 0)
            display_unit = request.form.get("display_unit", "").strip()
            conversion_factor = float(request.form.get("conversion_factor", 1) or 1)
            qty = display_qty * conversion_factor
            transfer_from = request.form.get("transfer_from", "").strip()
            transfer_to = request.form.get("transfer_to", "").strip()
            note = request.form.get("note", "").strip()

            conn.execute(
                """
                UPDATE inventory_transfers
                SET transfer_date = ?, qty = ?, display_qty = ?, display_unit = ?, conversion_factor = ?,
                    transfer_from = ?, transfer_to = ?, note = ?
                WHERE id = ?
                """,
                (transfer_date, qty, display_qty, display_unit, conversion_factor, transfer_from, transfer_to, note, transfer_id),
            )
            conn.commit()
            month = transfer_date[:7]

    items = conn.execute("SELECT id, name, unit, cost_per_unit FROM items ORDER BY name").fetchall()

    rows = conn.execute(
        """
        SELECT t.id, t.transfer_date, t.qty, t.display_qty, t.display_unit, t.conversion_factor,
               t.transfer_from, t.transfer_to, t.note,
               i.name AS item_name, i.unit, i.cost_per_unit,
               (t.qty * i.cost_per_unit) AS extended_value
        FROM inventory_transfers t
        JOIN items i ON i.id = t.item_id
        WHERE substr(t.transfer_date, 1, 7) = ?
        ORDER BY t.transfer_date, i.name
        """,
        (month,),
    ).fetchall()
    conn.close()

    total_extended = sum(float(r["extended_value"] or 0) for r in rows)

    return render_template(
        "inventory_transfers.html",
        items=items,
        rows=rows,
        month=month,
        total_extended=total_extended,
    )


@app.route("/inventory/transfers/<int:transfer_id>/delete", methods=["POST"])
@login_required
def delete_inventory_transfer(transfer_id):
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    conn = get_conn()
    conn.execute("DELETE FROM inventory_transfers WHERE id = ?", (transfer_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("inventory_transfers", month=month))


@app.route("/inventory/snapshots")
@login_required
def inventory_snapshots():
    conn = get_conn()
    months = conn.execute("SELECT DISTINCT snapshot_month, snapshot_date FROM inventory_snapshots ORDER BY snapshot_date DESC").fetchall()
    
    selected_month = request.args.get("month")
    snapshots = []
    total_value = 0
    if not selected_month and months:
        selected_month = months[0]["snapshot_month"]
        
    if selected_month:
        snapshots = conn.execute("SELECT * FROM inventory_snapshots WHERE snapshot_month = ? ORDER BY item_name", (selected_month,)).fetchall()
        total_value = sum(s["total_value"] for s in snapshots)
        
    return render_template("inventory_snapshots.html", months=months, snapshots=snapshots, selected_month=selected_month, total_value=total_value)

@app.route("/inventory/close_month", methods=["POST"])
@login_required
def close_month():
    conn = get_conn()
    month_name = request.form.get("month_name", datetime.now().strftime("%B %Y")).strip()
    if not month_name:
        month_name = datetime.now().strftime("%B %Y")
    snapshot_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    items = conn.execute("SELECT * FROM items").fetchall()
    for item in items:
        total_val = item["on_hand"] * item["cost_per_unit"]
        conn.execute(
            """
            INSERT INTO inventory_snapshots (snapshot_date, snapshot_month, item_name, quantity, unit, cost_per_unit, total_value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (snapshot_date, month_name, item["name"], item["on_hand"], item["unit"], item["cost_per_unit"], total_val)
        )
    conn.commit()
    return redirect(url_for('inventory_snapshots', month=month_name))


@app.route("/waste")
@login_required
def waste_log():
    month = (request.args.get("month") or datetime.now().strftime("%Y-%m"))
    
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT w.id, w.date, w.qty, w.unit, w.cost_per_unit_at_time, w.reason, w.total_value,
               i.name AS item_name
        FROM waste_log w
        JOIN items i ON i.id = w.item_id
        WHERE substr(w.date, 1, 7) = ?
        ORDER BY w.date DESC, w.id DESC
        """, (month,)
    ).fetchall()
    
    items = conn.execute("SELECT id, name, unit, cost_per_unit FROM items ORDER BY name").fetchall()
    conn.close()
    
    total_value = sum(float(r["total_value"] or 0) for r in rows)
    
    return render_template(
        "waste.html",
        rows=rows,
        month=month,
        total_value=total_value,
        items=items
    )

@app.route("/waste/add", methods=["POST"])
@login_required
def waste_add():
    conn = get_conn()
    item_id = int(request.form["item_id"])
    qty = float(request.form.get("qty", 0))
    date = request.form.get("date") or datetime.now().strftime("%Y-%m-%d")
    reason = request.form.get("reason", "").strip()
    
    item = conn.execute("SELECT name, unit, cost_per_unit FROM items WHERE id = ?", (item_id,)).fetchone()
    if item:
        cost_per_unit_at_time = item["cost_per_unit"]
        total_value = qty * cost_per_unit_at_time
        unit = item["unit"]
        
        conn.execute(
            """
            INSERT INTO waste_log (item_id, date, qty, unit, cost_per_unit_at_time, reason, total_value)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (item_id, date, qty, unit, cost_per_unit_at_time, reason, total_value)
        )
        
        # Deduct from inventory
        conn.execute("UPDATE items SET on_hand = on_hand - ? WHERE id = ?", (qty, item_id))
        conn.commit()
        
    conn.close()
    month = date[:7]
    return redirect(url_for("waste_log", month=month))

@app.route("/waste/<int:waste_id>/delete", methods=["POST"])
@login_required
def delete_waste(waste_id):
    month = request.args.get("month") or datetime.now().strftime("%Y-%m")
    conn = get_conn()
    conn.execute("DELETE FROM waste_log WHERE id = ?", (waste_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("waste_log", month=month))

@app.route("/menus/builder", methods=["GET", "POST"])
@app.route("/orders/from-menu", methods=["GET", "POST"])
@login_required
def order_from_menu():
    results = []
    missing = []
    raw_menu = ""
    people_count = 1
    guest_buffer_pct = 10.0
    vendor_groups = {}
    selected_recipe_rows = [{"recipe_id": "", "servings": ""}]

    conn = get_conn()
    items = conn.execute("SELECT * FROM items ORDER BY name").fetchall()
    recipes = conn.execute("SELECT id, name, servings FROM recipes ORDER BY name").fetchall()
    conn.close()

    by_name = {i["name"].strip().lower(): i for i in items}
    by_id = {str(i["id"]): i for i in items}

    if request.method == "POST":
        raw_menu = request.form.get("menu_text", "")
        try:
            people_count = float(request.form.get("people_count", 1) or 1)
        except ValueError:
            people_count = 1

        try:
            guest_buffer_pct = float(request.form.get("guest_buffer_pct", 10) or 10)
        except ValueError:
            guest_buffer_pct = 10

        effective_people = max(0, people_count) * (1 + max(0, guest_buffer_pct) / 100.0)

        # Recipe menu mode: choose recipes + target servings.
        recipe_ids = request.form.getlist("recipe_id")
        recipe_servings = request.form.getlist("recipe_target_servings")
        selected_recipe_rows = []
        conn = get_conn()
        for recipe_id, target_raw in zip(recipe_ids, recipe_servings):
            recipe_id = (recipe_id or "").strip()
            target_raw = (target_raw or "").strip()
            selected_recipe_rows.append({"recipe_id": recipe_id, "servings": target_raw})
            if not recipe_id:
                continue
            try:
                target_servings = float(target_raw or 0)
            except ValueError:
                target_servings = 0
            if target_servings <= 0:
                continue

            recipe = conn.execute("SELECT id, name, servings FROM recipes WHERE id = ?", (int(recipe_id),)).fetchone()
            if not recipe:
                continue
            base_servings = float(recipe["servings"] or 1) or 1
            scale = target_servings / base_servings
            ing_rows = conn.execute(
                """
                SELECT ri.quantity, i.name, i.unit, i.vendor, i.on_hand, i.cost_per_unit
                FROM recipe_ingredients ri
                JOIN items i ON i.id = ri.item_id
                WHERE ri.recipe_id = ?
                """,
                (recipe["id"],),
            ).fetchall()
            for ing in ing_rows:
                qty_needed = max(0, float(ing["quantity"] or 0) * scale)
                on_hand = float(ing["on_hand"] or 0)
                to_order = max(0, qty_needed - on_hand)
                est_cost = to_order * float(ing["cost_per_unit"] or 0)
                results.append({
                    "name": ing["name"],
                    "unit": ing["unit"],
                    "vendor": ing["vendor"],
                    "qty_needed": round(qty_needed, 2),
                    "on_hand": on_hand,
                    "to_order": round(to_order, 2),
                    "est_cost": round(est_cost, 2),
                })
        conn.close()
        if not selected_recipe_rows:
            selected_recipe_rows = [{"recipe_id": "", "servings": ""}]

        # Structured inventory mode: choose inventory items + qty per person.
        row_item_ids = request.form.getlist("row_item_id")
        row_qty_per_person = request.form.getlist("row_qty_per_person")

        for item_id, qty_pp_raw in zip(row_item_ids, row_qty_per_person):
            item_id = (item_id or "").strip()
            qty_pp_raw = (qty_pp_raw or "").strip()
            if not item_id:
                continue

            match = by_id.get(item_id)
            if not match:
                continue

            try:
                qty_per_person = float(qty_pp_raw or 0)
            except ValueError:
                qty_per_person = 0

            qty_needed = max(0, effective_people * qty_per_person)
            on_hand = float(match["on_hand"])
            to_order = max(0, qty_needed - on_hand)
            est_cost = to_order * float(match["cost_per_unit"])

            results.append({
                "name": match["name"],
                "unit": match["unit"],
                "vendor": match["vendor"],
                "qty_needed": round(qty_needed, 2),
                "on_hand": on_hand,
                "to_order": round(to_order, 2),
                "est_cost": round(est_cost, 2),
            })

        # Legacy quick-paste mode remains supported.
        for line in raw_menu.splitlines():
            line = line.strip()
            if not line:
                continue

            parts = [p.strip() for p in line.split(",")]
            name = parts[0] if parts else ""
            qty_needed = 1.0
            if len(parts) > 1 and parts[1]:
                try:
                    qty_needed = float(parts[1])
                except ValueError:
                    qty_needed = 1.0

            key = name.lower()
            match = by_name.get(key)
            if not match:
                missing.append({"name": name, "qty_needed": qty_needed})
                continue

            on_hand = float(match["on_hand"])
            to_order = max(0, qty_needed - on_hand)
            est_cost = to_order * float(match["cost_per_unit"])

            results.append({
                "name": match["name"],
                "unit": match["unit"],
                "vendor": match["vendor"],
                "qty_needed": qty_needed,
                "on_hand": on_hand,
                "to_order": round(to_order, 2),
                "est_cost": round(est_cost, 2),
            })

        # De-duplicate same items coming from both modes.
        dedup = {}
        for r in results:
            key = r["name"].strip().lower()
            if key not in dedup:
                dedup[key] = dict(r)
            else:
                dedup[key]["qty_needed"] += float(r["qty_needed"])
                dedup[key]["to_order"] += float(r["to_order"])
                dedup[key]["est_cost"] += float(r["est_cost"])

        results = []
        for r in dedup.values():
            r["qty_needed"] = round(float(r["qty_needed"]), 2)
            r["to_order"] = round(float(r["to_order"]), 2)
            r["est_cost"] = round(float(r["est_cost"]), 2)
            results.append(r)

        results.sort(key=lambda x: (x.get("vendor") or "", x["name"]))

        if request.form.get("export_csv") == "1":
            output = io.StringIO()
            writer = csv.writer(output)
            writer.writerow(["Vendor", "Item", "Qty Needed", "On Hand", "Order Qty", "Unit", "Est Cost"])
            for r in results:
                writer.writerow([
                    r.get("vendor") or "",
                    r["name"],
                    r["qty_needed"],
                    r["on_hand"],
                    r["to_order"],
                    r["unit"],
                    r["est_cost"],
                ])
            return Response(
                output.getvalue(),
                mimetype="text/csv",
                headers={"Content-Disposition": "attachment; filename=menu_based_order.csv"},
            )

        if request.form.get("export_vendor_csv") == "1":
            grouped = defaultdict(list)
            for r in results:
                grouped[r.get("vendor") or "Unassigned Vendor"].append(r)

            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
                for vendor, rows in grouped.items():
                    safe_vendor = re.sub(r"[^A-Za-z0-9_-]+", "_", vendor).strip("_") or "vendor"
                    csv_io = io.StringIO()
                    writer = csv.writer(csv_io)
                    writer.writerow(["Vendor", "Item", "Qty Needed", "On Hand", "Order Qty", "Unit", "Est Cost"])
                    for r in rows:
                        writer.writerow([
                            r.get("vendor") or "",
                            r["name"],
                            r["qty_needed"],
                            r["on_hand"],
                            r["to_order"],
                            r["unit"],
                            r["est_cost"],
                        ])
                    zf.writestr(f"PO_{safe_vendor}.csv", csv_io.getvalue())

            zip_buffer.seek(0)
            return Response(
                zip_buffer.getvalue(),
                mimetype="application/zip",
                headers={"Content-Disposition": "attachment; filename=vendor_purchase_sheets.zip"},
            )

    grouped = defaultdict(list)
    for r in results:
        grouped[r.get("vendor") or "Unassigned Vendor"].append(r)

    vendor_groups = {
        vendor: {
            "rows": rows,
            "total": round(sum(float(x["est_cost"]) for x in rows), 2),
        }
        for vendor, rows in grouped.items()
    }

    total_est = round(sum(r["est_cost"] for r in results), 2)
    return render_template(
        "menu_order.html",
        raw_menu=raw_menu,
        results=results,
        missing=missing,
        total_est=total_est,
        items=items,
        recipes=recipes,
        selected_recipe_rows=selected_recipe_rows,
        people_count=people_count,
        guest_buffer_pct=guest_buffer_pct,
        vendor_groups=vendor_groups,
    )


@app.route("/orders/suggested.csv")
@login_required
def suggested_order_csv():
    suggested = build_suggested_rows()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Item", "Vendor", "Qty to Order", "Unit", "Estimated Cost"])
    for s in suggested:
        writer.writerow([s["name"], s["vendor"], s["order_qty"], s["unit"], s["est_cost"]])

    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=suggested_order.csv"},
    )


@app.route("/recipes", methods=["GET", "POST"])
@login_required
def recipes():
    conn = get_conn()
    if request.method == "POST":
        name = request.form["name"].strip()
        servings = float(request.form.get("servings", 1) or 1)
        sale_price = float(request.form.get("sale_price", 0) or 0)
        allergens = parse_allergens(request.form)
        dietary_labels = parse_dietary(request.form)

        image_path = None
        image = request.files.get("image")
        if image and image.filename:
            safe = secure_filename(image.filename)
            filename = f"recipe_{name.lower().replace(' ', '_')}_{safe}"
            dest = UPLOADS_DIR / filename
            image.save(dest)
            image_path = filename

        conn.execute(
            "INSERT INTO recipes (name, servings, sale_price, image_path, allergens, dietary_labels) VALUES (?, ?, ?, ?, ?, ?)",
            (name, servings, sale_price, image_path, allergens, dietary_labels),
        )
        conn.commit()

    recipes_list = conn.execute("SELECT * FROM recipes ORDER BY name").fetchall()
    conn.close()
    return render_template(
        "recipes.html",
        recipes=recipes_list,
        common_allergens=COMMON_ALLERGENS,
        dietary_labels=DIETARY_LABELS,
    )


@app.route("/recipes/<int:recipe_id>/delete", methods=["POST"])
@login_required
def delete_recipe(recipe_id):
    conn = get_conn()
    conn.execute("DELETE FROM recipe_ingredients WHERE recipe_id = ?", (recipe_id,))
    conn.execute("DELETE FROM recipes WHERE id = ?", (recipe_id,))
    conn.commit()
    conn.close()
    return redirect(url_for("recipes"))


@app.route("/recipes/<int:recipe_id>", methods=["GET", "POST"])
@login_required
def recipe_detail(recipe_id):
    conn = get_conn()

    if request.method == "POST":
        action = request.form.get("action", "add")

        if action == "add":
            item_id = int(request.form["item_id"])
            display_quantity = float(request.form["quantity"])
            display_unit = request.form.get("display_unit", "").strip()
            conversion_factor = float(request.form.get("conversion_factor", 1) or 1)
            quantity = display_quantity * conversion_factor

            conn.execute(
                """
                INSERT INTO recipe_ingredients
                (recipe_id, item_id, quantity, display_quantity, display_unit, conversion_factor)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (recipe_id, item_id, quantity, display_quantity, display_unit, conversion_factor),
            )
            conn.commit()

        elif action == "update":
            ingredient_id = int(request.form["ingredient_id"])
            display_quantity = float(request.form["quantity"])
            display_unit = request.form.get("display_unit", "").strip()
            conversion_factor = float(request.form.get("conversion_factor", 1) or 1)
            quantity = display_quantity * conversion_factor

            conn.execute(
                """
                UPDATE recipe_ingredients
                SET quantity = ?, display_quantity = ?, display_unit = ?, conversion_factor = ?
                WHERE id = ? AND recipe_id = ?
                """,
                (quantity, display_quantity, display_unit, conversion_factor, ingredient_id, recipe_id),
            )
            conn.commit()

        elif action == "delete":
            ingredient_id = int(request.form["ingredient_id"])
            conn.execute(
                "DELETE FROM recipe_ingredients WHERE id = ? AND recipe_id = ?",
                (ingredient_id, recipe_id),
            )
            conn.commit()

        elif action == "update_recipe":
            servings = float(request.form.get("servings", 1) or 1)
            sale_price = float(request.form.get("sale_price", 0) or 0)

            current = conn.execute(
                "SELECT image_path, name, allergens, dietary_labels FROM recipes WHERE id = ?",
                (recipe_id,),
            ).fetchone()
            image_path = current["image_path"] if current else None

            allergens = parse_allergens(request.form)
            dietary_labels = parse_dietary(request.form)
            # If browser submits settings without allergen/dietary controls, preserve existing values.
            if not allergens and ("allergens_list" not in request.form and "allergens_custom" not in request.form):
                allergens = (current["allergens"] if current else "") or ""
            if not dietary_labels and ("dietary_list" not in request.form and "dietary_custom" not in request.form):
                dietary_labels = (current["dietary_labels"] if current else "") or ""

            image = request.files.get("image")
            if image and image.filename:
                safe = secure_filename(image.filename)
                filename = f"recipe_{(current['name'] if current else recipe_id).lower().replace(' ', '_')}_{safe}"
                dest = UPLOADS_DIR / filename
                image.save(dest)
                image_path = filename

            conn.execute(
                "UPDATE recipes SET servings = ?, sale_price = ?, image_path = ?, allergens = ?, dietary_labels = ? WHERE id = ?",
                (servings, sale_price, image_path, allergens, dietary_labels, recipe_id),
            )
            conn.commit()

    recipe = conn.execute("SELECT * FROM recipes WHERE id = ?", (recipe_id,)).fetchone()
    items = conn.execute("SELECT * FROM items ORDER BY name").fetchall()
    units = [r["unit"] for r in conn.execute("SELECT DISTINCT unit FROM items ORDER BY unit").fetchall()]

    ingredients = conn.execute(
        """
        SELECT ri.id, ri.quantity, ri.display_quantity, ri.display_unit, ri.conversion_factor,
               i.name, i.unit, i.cost_per_unit
        FROM recipe_ingredients ri
        JOIN items i ON i.id = ri.item_id
        WHERE ri.recipe_id = ?
        ORDER BY i.name
        """,
        (recipe_id,),
    ).fetchall()

    total_cost = sum(row["quantity"] * row["cost_per_unit"] for row in ingredients)
    cost_per_serving = (total_cost / recipe["servings"]) if recipe and recipe["servings"] else 0
    sale_price = recipe["sale_price"] if recipe else 0
    food_cost_pct = ((cost_per_serving / sale_price) * 100) if sale_price else None

    listed_allergens = [a.strip() for a in (recipe["allergens"] or "").split(",") if a.strip()] if recipe else []
    common_lower = {a.lower() for a in COMMON_ALLERGENS}
    selected_common = [a for a in listed_allergens if a.lower() in common_lower]
    custom_allergens = ", ".join([a for a in listed_allergens if a.lower() not in common_lower])

    listed_dietary = [d.strip() for d in (recipe["dietary_labels"] or "").split(",") if d.strip()] if recipe else []
    dietary_lower = {d.lower() for d in DIETARY_LABELS}
    selected_dietary = [d for d in listed_dietary if d.lower() in dietary_lower]
    custom_dietary = ", ".join([d for d in listed_dietary if d.lower() not in dietary_lower])

    conn.close()
    return render_template(
        "recipe_detail.html",
        recipe=recipe,
        items=items,
        units=units,
        ingredients=ingredients,
        total_cost=total_cost,
        cost_per_serving=cost_per_serving,
        food_cost_pct=food_cost_pct,
        common_allergens=COMMON_ALLERGENS,
        selected_common=selected_common,
        custom_allergens=custom_allergens,
        dietary_labels=DIETARY_LABELS,
        selected_dietary=selected_dietary,
        custom_dietary=custom_dietary,
    )


@app.route("/reports/month_end")
@login_required
def month_end_summary():
    conn = get_conn()
    month_val = request.args.get("month", datetime.now().strftime("%Y-%m"))
    try:
        month_name = datetime.strptime(month_val, "%Y-%m").strftime("%B %Y")
    except ValueError:
        month_name = month_val

    # 1. Total Purchases
    purchases_row = conn.execute("SELECT SUM(invoice_total) as total FROM purchases WHERE strftime('%Y-%m', purchase_date) = ?", (month_val,)).fetchone()
    purchases_total = float(purchases_row["total"] or 0)

    # 2. Total Waste
    waste_rows = conn.execute("SELECT total_value FROM waste_log WHERE substr(date, 1, 7) = ?", (month_val,)).fetchall()
    waste_total = sum(float(r["total_value"] or 0) for r in waste_rows)

    # 3. Total Transfers (Deductions)
    transfer_rows = conn.execute(
        """
        SELECT (t.qty * i.cost_per_unit) AS extended_value 
        FROM inventory_transfers t 
        JOIN items i ON i.id = t.item_id 
        WHERE substr(t.transfer_date, 1, 7) = ?
        """, (month_val,)
    ).fetchall()
    transfers_total = sum(float(r["extended_value"] or 0) for r in transfer_rows)

    # 4. Ending Inventory Snapshot
    inv_rows = conn.execute("SELECT total_value FROM inventory_snapshots WHERE snapshot_month = ? OR snapshot_month = ?", (month_name, month_val)).fetchall()
    inventory_total = sum(float(r["total_value"] or 0) for r in inv_rows)

    return render_template(
        "month_end_summary.html",
        month_val=month_val,
        month_name=month_name,
        purchases_total=purchases_total,
        waste_total=waste_total,
        transfers_total=transfers_total,
        inventory_total=inventory_total
    )

@app.route("/purchases", methods=["GET", "POST"])
@login_required
def purchases():
    conn = get_conn()
    month = request.args.get("month", datetime.now().strftime("%Y-%m"))
    
    if request.method == "POST":
        # Handle setting the budget
        if "set_budget" in request.form:
            try:
                budget_amount = float(request.form.get("budget_amount", 0))
            except ValueError:
                budget_amount = 0.0
            
            # Using INSERT OR REPLACE (SQLite)
            conn.execute(
                "INSERT INTO monthly_budgets (month, budget) VALUES (?, ?) ON CONFLICT(month) DO UPDATE SET budget=excluded.budget",
                (month, budget_amount)
            )
            conn.commit()
            return redirect(url_for("purchases", month=month))
            
        # Handle logging an invoice
        purchase_date = request.form.get("purchase_date")
        company = request.form.get("company")
        invoice_number = request.form.get("invoice_number", "")
        po_number = request.form.get("po_number", "")
        try:
            invoice_total = float(request.form.get("invoice_total", 0))
        except ValueError:
            invoice_total = 0.0
            
        if purchase_date and company:
            conn.execute(
                """
                INSERT INTO purchases (purchase_date, company, invoice_number, invoice_total, po_number)
                VALUES (?, ?, ?, ?, ?)
                """,
                (purchase_date, company, invoice_number, invoice_total, po_number)
            )
            conn.commit()
            return redirect(url_for("purchases", month=month))
            
    # GET method - fetch purchases
    purchases_data = conn.execute(
        "SELECT * FROM purchases WHERE strftime('%Y-%m', purchase_date) = ? ORDER BY purchase_date DESC", 
        (month,)
    ).fetchall()
    
    # Fetch current budget
    budget_row = conn.execute("SELECT budget FROM monthly_budgets WHERE month = ?", (month,)).fetchone()
    current_budget = float(budget_row["budget"]) if budget_row else 0.0
    
    # Calculate total spend
    total_spend = sum(p["invoice_total"] for p in purchases_data)
    remaining_budget = current_budget - total_spend
    
    return render_template("purchases.html", purchases=purchases_data, total_spend=total_spend, month=month, current_budget=current_budget, remaining_budget=remaining_budget)


if __name__ == "__main__":
    ensure_inventory_dbs()
    for inv in INVENTORIES.values():
        init_db(inv["db_path"])
    app.run(host="0.0.0.0", port=5000, debug=True)
