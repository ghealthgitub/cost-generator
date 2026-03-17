"""
🫚 Ginger Universe — Cost Generator DB Connector
Shared PostgreSQL: reads treatments/specialties/hospitals, manages cost tables
"""

import psycopg2
import psycopg2.extras
import config
import bcrypt
import json
from datetime import datetime


def get_conn():
    if not config.DATABASE_URL:
        return None
    try:
        return psycopg2.connect(config.DATABASE_URL)
    except Exception as e:
        print(f"[DB Error] {e}")
        return None


# ═══════════════════════════════════════════════════════════════
# BOOTSTRAP — Create cost-specific tables (won't touch existing)
# ═══════════════════════════════════════════════════════════════

DEFAULT_EXTRACTION_PROMPT = """You are a medical cost data extraction specialist for Ginger Healthcare.

Extract structured hospital pricing data from the provided content. Return ONLY valid JSON array.

For each treatment/procedure found, return:
{
  "treatment_name": "exact name as shown",
  "hospital_name": "hospital name if identifiable",
  "city": "city if identifiable",
  "total_cost_min": number in INR (or null),
  "total_cost_max": number in INR (or null),
  "currency": "INR",
  "stay_days_min": number or null,
  "stay_days_max": number or null,
  "is_daycare": true/false or null,
  "room_type": "economy/shared/private/not_specified",
  "components": [
    {"type": "surgeon_fee", "amount_min": num, "amount_max": num},
    {"type": "anaesthesia", "amount_min": num, "amount_max": num},
    {"type": "ot_charges", "amount_min": num, "amount_max": num},
    {"type": "implant_device", "amount_min": num, "amount_max": num},
    {"type": "icu_per_day", "amount_min": num, "amount_max": num, "quantity": days},
    {"type": "ward_per_day", "amount_min": num, "amount_max": num, "quantity": days},
    {"type": "diagnostics", "amount_min": num, "amount_max": num},
    {"type": "medications", "amount_min": num, "amount_max": num},
    {"type": "physiotherapy", "amount_min": num, "amount_max": num},
    {"type": "nursing_misc", "amount_min": num, "amount_max": num}
  ],
  "notes": "any relevant notes about inclusions/exclusions"
}

Only include components where data is available. Convert all amounts to INR.
If a single price is given (not a range), use it for both min and max.
Return a JSON array of all treatments found. Return ONLY the JSON, no other text."""

DEFAULT_GENERATION_PROMPT = """You are a medical cost content writer for Ginger Healthcare (ginger.healthcare), India's trusted medical tourism platform.

You will receive:
1. A treatment article from our website
2. Real hospital pricing data we've collected (the "calibration data")

Your job: Generate an HTML cost section for this treatment page.

RULES:
- Read the article carefully to understand: what the procedure involves, typical hospital stay, whether implants/devices are needed, complexity level, recovery time
- Use the calibration data to ground your estimates in real Indian hospital pricing
- If calibration data exists for this exact treatment, use those ranges directly
- If not, reason from similar procedures and component costs
- All costs in INR (₹), show ranges to account for hospital tier and city variation
- Mention what causes cost variation (city, hospital tier, implant type, room category, etc.)
- Be specific to THIS treatment — don't write generic cost content

OUTPUT FORMAT — Return ONLY this HTML structure, no markdown fences, no extra text:

<div class="cost-section">
  <p class="cost-summary">[Opening paragraph: 2-3 sentences explaining the cost range and key drivers for THIS specific treatment. Be specific — mention the actual procedure details that affect cost.]</p>

  <table class="cost-table">
    <thead>
      <tr><th>Component</th><th>Estimated Cost (₹)</th></tr>
    </thead>
    <tbody>
      <tr><td>[Component name]</td><td>₹XX,XXX – ₹XX,XXX</td></tr>
      <!-- Include ALL relevant components for this treatment -->
    </tbody>
    <tfoot>
      <tr><td><strong>Estimated Total</strong></td><td><strong>₹X,XX,XXX – ₹X,XX,XXX</strong></td></tr>
    </tfoot>
  </table>

  <p class="cost-note">[Closing note: what factors cause the cost to be at the lower vs higher end. Mention specific things like imported vs domestic implants, metro vs tier-2 city, economy vs premium room, etc. Keep it 1-2 sentences.]</p>
</div>

IMPORTANT:
- Use Indian number formatting: ₹1,50,000 not ₹150,000
- Only include components that are actually relevant to this treatment
- For daycare/OPD procedures, don't include room/stay components
- Be medically accurate about what the procedure requires"""


def init_cost_tables():
    """Create tables specific to the cost generator"""
    conn = get_conn()
    if not conn:
        print("⚠️  No DB — running without cost tables")
        return
    try:
        with conn.cursor() as cur:
            # ── Cost sources: tracks each upload/scrape ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cost_sources (
                    id SERIAL PRIMARY KEY,
                    source_type VARCHAR(20) NOT NULL,
                    source_name VARCHAR(500),
                    source_url TEXT,
                    file_data TEXT,
                    record_count INTEGER DEFAULT 0,
                    status VARCHAR(30) DEFAULT 'pending',
                    uploaded_by VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # ── Cost hospital data: real pricing entries ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cost_hospital_data (
                    id SERIAL PRIMARY KEY,
                    treatment_id INTEGER,
                    treatment_name_raw VARCHAR(500),
                    hospital_id INTEGER,
                    hospital_name_raw VARCHAR(500),
                    country VARCHAR(100) DEFAULT 'India',
                    city VARCHAR(200),
                    currency VARCHAR(10) DEFAULT 'INR',
                    total_cost_min NUMERIC(12,2),
                    total_cost_max NUMERIC(12,2),
                    usd_equivalent_min NUMERIC(12,2),
                    usd_equivalent_max NUMERIC(12,2),
                    stay_days_min INTEGER,
                    stay_days_max INTEGER,
                    is_daycare BOOLEAN DEFAULT false,
                    room_type VARCHAR(50) DEFAULT 'not_specified',
                    source_id INTEGER REFERENCES cost_sources(id),
                    data_type VARCHAR(30) DEFAULT 'published',
                    confidence VARCHAR(20) DEFAULT 'high',
                    notes TEXT,
                    created_by VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # ── Cost components: breakdown per entry ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cost_components (
                    id SERIAL PRIMARY KEY,
                    cost_entry_id INTEGER REFERENCES cost_hospital_data(id) ON DELETE CASCADE,
                    component_type VARCHAR(50) NOT NULL,
                    amount_min NUMERIC(12,2),
                    amount_max NUMERIC(12,2),
                    quantity INTEGER DEFAULT 1,
                    notes VARCHAR(500)
                )
            """)

            # ── Cost generated: HTML cost sections for treatments ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cost_generated (
                    id SERIAL PRIMARY KEY,
                    treatment_id INTEGER NOT NULL,
                    treatment_name VARCHAR(500),
                    specialty_name VARCHAR(300),
                    generated_html TEXT,
                    edited_html TEXT,
                    prompt_used TEXT,
                    calibration_summary TEXT,
                    status VARCHAR(30) DEFAULT 'draft',
                    generated_by VARCHAR(255),
                    reviewed_by VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # ── Cost prompts: editable extraction + generation prompts ──
            cur.execute("""
                CREATE TABLE IF NOT EXISTS cost_prompts (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    prompt_type VARCHAR(30) NOT NULL,
                    prompt_text TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT false,
                    created_by VARCHAR(255),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)

            # ── Seed default prompts if none exist ──
            cur.execute("SELECT COUNT(*) FROM cost_prompts WHERE prompt_type = 'extraction' AND is_active = true")
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO cost_prompts (name, prompt_type, prompt_text, is_active, created_by)
                    VALUES ('Default Extraction Prompt', 'extraction', %s, true, 'system')
                """, [DEFAULT_EXTRACTION_PROMPT])

            cur.execute("SELECT COUNT(*) FROM cost_prompts WHERE prompt_type = 'generation' AND is_active = true")
            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO cost_prompts (name, prompt_type, prompt_text, is_active, created_by)
                    VALUES ('Default Generation Prompt', 'generation', %s, true, 'system')
                """, [DEFAULT_GENERATION_PROMPT])

            conn.commit()
        print("✅ Cost generator tables ready")
    except Exception as e:
        print(f"[DB Init Error] {e}")
        conn.rollback()
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# AUTH — Shared with profile generator
# ═══════════════════════════════════════════════════════════════

def authenticate_user(email, password):
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, name, email, password_hash, role, is_active
                FROM users WHERE email = %s AND is_active = true
            """, [email])
            user = cur.fetchone()
            if user and bcrypt.checkpw(password.encode('utf-8'), user['password_hash'].encode('utf-8')):
                return {
                    'id': user['id'], 'name': user['name'],
                    'email': user['email'], 'role': user['role']
                }
    except Exception as e:
        print(f"[Auth Error] {e}")
    finally:
        conn.close()
    return None


# ═══════════════════════════════════════════════════════════════
# READ EXISTING TABLES — Treatments, Specialties, Hospitals
# ═══════════════════════════════════════════════════════════════

def get_specialties_list():
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name FROM specialties WHERE is_active = true ORDER BY name")
            return cur.fetchall()
    except Exception as e:
        print(f"[Specialties Error] {e}")
        return []
    finally:
        conn.close()


def get_treatments_by_specialty(specialty_id=None):
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if specialty_id:
                cur.execute("""
                    SELECT t.id, t.name, t.slug, s.name as specialty_name,
                           t.description, t.content
                    FROM treatments t
                    LEFT JOIN specialties s ON t.specialty_id = s.id
                    WHERE t.specialty_id = %s AND t.is_active = true
                    ORDER BY t.name
                """, [specialty_id])
            else:
                cur.execute("""
                    SELECT t.id, t.name, t.slug, s.name as specialty_name,
                           t.description, t.content
                    FROM treatments t
                    LEFT JOIN specialties s ON t.specialty_id = s.id
                    WHERE t.is_active = true
                    ORDER BY s.name, t.name
                """)
            return cur.fetchall()
    except Exception as e:
        print(f"[Treatments Error] {e}")
        return []
    finally:
        conn.close()


def get_treatment_by_id(treatment_id):
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT t.id, t.name, t.slug, t.description, t.content,
                       s.name as specialty_name, s.id as specialty_id
                FROM treatments t
                LEFT JOIN specialties s ON t.specialty_id = s.id
                WHERE t.id = %s
            """, [treatment_id])
            return cur.fetchone()
    except Exception as e:
        print(f"[Treatment Error] {e}")
        return None
    finally:
        conn.close()


def get_hospitals_list():
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id, name, city FROM hospitals WHERE is_active = true ORDER BY name")
            return cur.fetchall()
    except Exception as e:
        print(f"[Hospitals Error] {e}")
        return []
    finally:
        conn.close()


def search_treatments(query):
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT t.id, t.name, t.slug, s.name as specialty_name
                FROM treatments t
                LEFT JOIN specialties s ON t.specialty_id = s.id
                WHERE t.is_active = true AND t.name ILIKE %s
                ORDER BY t.name LIMIT 20
            """, [f"%{query}%"])
            return cur.fetchall()
    except Exception as e:
        print(f"[Search Error] {e}")
        return []
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# COST SOURCES — Upload tracking
# ═══════════════════════════════════════════════════════════════

def save_cost_source(source_type, source_name, source_url, record_count, uploaded_by, status='completed'):
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO cost_sources (source_type, source_name, source_url, record_count, status, uploaded_by)
                VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
            """, [source_type, source_name, source_url, record_count, status, uploaded_by])
            source_id = cur.fetchone()[0]
            conn.commit()
            return source_id
    except Exception as e:
        print(f"[Save Source Error] {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def get_recent_sources(limit=20):
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT id, source_type, source_name, source_url, record_count,
                       status, uploaded_by, created_at
                FROM cost_sources ORDER BY created_at DESC LIMIT %s
            """, [limit])
            return cur.fetchall()
    except Exception as e:
        print(f"[Sources Error] {e}")
        return []
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# COST HOSPITAL DATA — Real pricing entries
# ═══════════════════════════════════════════════════════════════

def save_cost_entry(entry, source_id, created_by):
    """Save a single cost entry with its components"""
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            # Try to match treatment to existing
            treatment_id = None
            if entry.get('treatment_name'):
                cur.execute(
                    "SELECT id FROM treatments WHERE name ILIKE %s LIMIT 1",
                    [f"%{entry['treatment_name']}%"]
                )
                row = cur.fetchone()
                if row:
                    treatment_id = row[0]

            # Try to match hospital
            hospital_id = None
            if entry.get('hospital_name'):
                cur.execute(
                    "SELECT id FROM hospitals WHERE name ILIKE %s LIMIT 1",
                    [f"%{entry['hospital_name']}%"]
                )
                row = cur.fetchone()
                if row:
                    hospital_id = row[0]

            # INR to USD rough conversion for normalization
            usd_min = round(entry.get('total_cost_min', 0) / 85, 2) if entry.get('total_cost_min') else None
            usd_max = round(entry.get('total_cost_max', 0) / 85, 2) if entry.get('total_cost_max') else None

            cur.execute("""
                INSERT INTO cost_hospital_data
                (treatment_id, treatment_name_raw, hospital_id, hospital_name_raw,
                 country, city, currency, total_cost_min, total_cost_max,
                 usd_equivalent_min, usd_equivalent_max,
                 stay_days_min, stay_days_max, is_daycare, room_type,
                 source_id, data_type, confidence, notes, created_by)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id
            """, [
                treatment_id, entry.get('treatment_name', ''),
                hospital_id, entry.get('hospital_name', ''),
                entry.get('country', 'India'), entry.get('city', ''),
                entry.get('currency', 'INR'),
                entry.get('total_cost_min'), entry.get('total_cost_max'),
                usd_min, usd_max,
                entry.get('stay_days_min'), entry.get('stay_days_max'),
                entry.get('is_daycare', False),
                entry.get('room_type', 'not_specified'),
                source_id, 'published', 'high',
                entry.get('notes', ''), created_by
            ])
            cost_entry_id = cur.fetchone()[0]

            # Save components
            for comp in entry.get('components', []):
                cur.execute("""
                    INSERT INTO cost_components
                    (cost_entry_id, component_type, amount_min, amount_max, quantity, notes)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, [
                    cost_entry_id, comp.get('type', ''),
                    comp.get('amount_min'), comp.get('amount_max'),
                    comp.get('quantity', 1), comp.get('notes', '')
                ])

            conn.commit()
            return cost_entry_id
    except Exception as e:
        print(f"[Save Entry Error] {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def save_cost_entries_batch(entries, source_id, created_by):
    """Save multiple cost entries from a single source"""
    saved = 0
    for entry in entries:
        result = save_cost_entry(entry, source_id, created_by)
        if result:
            saved += 1
    # Update source record count
    conn = get_conn()
    if conn:
        try:
            with conn.cursor() as cur:
                cur.execute("UPDATE cost_sources SET record_count = %s WHERE id = %s", [saved, source_id])
                conn.commit()
        except:
            conn.rollback()
        finally:
            conn.close()
    return saved


def get_cost_data_for_treatment(treatment_id=None, treatment_name=None):
    """Get all real pricing data for a treatment (for calibration)"""
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if treatment_id:
                cur.execute("""
                    SELECT chd.*, array_agg(
                        json_build_object(
                            'type', cc.component_type,
                            'amount_min', cc.amount_min,
                            'amount_max', cc.amount_max,
                            'quantity', cc.quantity
                        )
                    ) FILTER (WHERE cc.id IS NOT NULL) as components
                    FROM cost_hospital_data chd
                    LEFT JOIN cost_components cc ON cc.cost_entry_id = chd.id
                    WHERE chd.treatment_id = %s
                    GROUP BY chd.id
                    ORDER BY chd.created_at DESC
                """, [treatment_id])
            elif treatment_name:
                cur.execute("""
                    SELECT chd.*, array_agg(
                        json_build_object(
                            'type', cc.component_type,
                            'amount_min', cc.amount_min,
                            'amount_max', cc.amount_max,
                            'quantity', cc.quantity
                        )
                    ) FILTER (WHERE cc.id IS NOT NULL) as components
                    FROM cost_hospital_data chd
                    LEFT JOIN cost_components cc ON cc.cost_entry_id = chd.id
                    WHERE chd.treatment_name_raw ILIKE %s
                    GROUP BY chd.id
                    ORDER BY chd.created_at DESC
                """, [f"%{treatment_name}%"])
            else:
                return []
            return cur.fetchall()
    except Exception as e:
        print(f"[Cost Data Error] {e}")
        return []
    finally:
        conn.close()


def get_calibration_data_for_specialty(specialty_name):
    """Get all pricing data for a specialty (broader context for predictions)"""
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT chd.treatment_name_raw, chd.hospital_name_raw, chd.city,
                       chd.total_cost_min, chd.total_cost_max,
                       chd.stay_days_min, chd.stay_days_max, chd.is_daycare
                FROM cost_hospital_data chd
                LEFT JOIN treatments t ON chd.treatment_id = t.id
                LEFT JOIN specialties s ON t.specialty_id = s.id
                WHERE s.name ILIKE %s
                ORDER BY chd.treatment_name_raw
                LIMIT 100
            """, [f"%{specialty_name}%"])
            return cur.fetchall()
    except Exception as e:
        print(f"[Calibration Error] {e}")
        return []
    finally:
        conn.close()


def get_all_calibration_summary():
    """Get a summary of all pricing data for broad context"""
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT treatment_name_raw, hospital_name_raw, city,
                       total_cost_min, total_cost_max,
                       stay_days_min, stay_days_max, is_daycare, room_type
                FROM cost_hospital_data
                ORDER BY treatment_name_raw
                LIMIT 500
            """)
            return cur.fetchall()
    except Exception as e:
        print(f"[Calibration Summary Error] {e}")
        return []
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# COST GENERATED — HTML cost sections for treatment pages
# ═══════════════════════════════════════════════════════════════

def save_generated_cost(treatment_id, treatment_name, specialty_name,
                        generated_html, prompt_used, calibration_summary, generated_by):
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor() as cur:
            # Check if one exists already
            cur.execute("SELECT id FROM cost_generated WHERE treatment_id = %s", [treatment_id])
            existing = cur.fetchone()
            if existing:
                cur.execute("""
                    UPDATE cost_generated
                    SET generated_html = %s, edited_html = NULL, prompt_used = %s,
                        calibration_summary = %s, status = 'draft',
                        generated_by = %s, updated_at = NOW()
                    WHERE treatment_id = %s RETURNING id
                """, [generated_html, prompt_used, calibration_summary,
                      generated_by, treatment_id])
                result_id = cur.fetchone()[0]
            else:
                cur.execute("""
                    INSERT INTO cost_generated
                    (treatment_id, treatment_name, specialty_name,
                     generated_html, prompt_used, calibration_summary, generated_by)
                    VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
                """, [treatment_id, treatment_name, specialty_name,
                      generated_html, prompt_used, calibration_summary, generated_by])
                result_id = cur.fetchone()[0]
            conn.commit()
            return result_id
    except Exception as e:
        print(f"[Save Generated Error] {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def update_generated_cost(gen_id, edited_html, status, reviewed_by=None):
    conn = get_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE cost_generated
                SET edited_html = %s, status = %s, reviewed_by = %s, updated_at = NOW()
                WHERE id = %s
            """, [edited_html, status, reviewed_by, gen_id])
            conn.commit()
            return True
    except Exception as e:
        print(f"[Update Generated Error] {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def get_generated_cost(treatment_id):
    conn = get_conn()
    if not conn:
        return None
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM cost_generated WHERE treatment_id = %s
                ORDER BY updated_at DESC LIMIT 1
            """, [treatment_id])
            return cur.fetchone()
    except Exception as e:
        print(f"[Get Generated Error] {e}")
        return None
    finally:
        conn.close()


def get_generation_stats():
    conn = get_conn()
    if not conn:
        return {'total_treatments': 0, 'generated': 0, 'approved': 0, 'data_points': 0, 'hospitals_covered': 0}
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM treatments WHERE is_active = true")
            total = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cost_generated")
            generated = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cost_generated WHERE status = 'approved'")
            approved = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cost_hospital_data")
            data_points = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT hospital_name_raw) FROM cost_hospital_data")
            hospitals = cur.fetchone()[0]

            cur.execute("SELECT COUNT(DISTINCT treatment_name_raw) FROM cost_hospital_data")
            treatments_with_data = cur.fetchone()[0]

            cur.execute("SELECT COUNT(*) FROM cost_sources")
            sources = cur.fetchone()[0]

            return {
                'total_treatments': total,
                'generated': generated,
                'approved': approved,
                'data_points': data_points,
                'hospitals_covered': hospitals,
                'treatments_with_data': treatments_with_data,
                'sources': sources
            }
    except Exception as e:
        print(f"[Stats Error] {e}")
        return {'total_treatments': 0, 'generated': 0, 'approved': 0, 'data_points': 0, 'hospitals_covered': 0, 'treatments_with_data': 0, 'sources': 0}
    finally:
        conn.close()


def get_specialty_coverage():
    """Get per-specialty breakdown of cost generation progress"""
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT s.id, s.name,
                       COUNT(DISTINCT t.id) as total_treatments,
                       COUNT(DISTINCT cg.treatment_id) as generated_count,
                       COUNT(DISTINCT CASE WHEN cg.status = 'approved' THEN cg.treatment_id END) as approved_count
                FROM specialties s
                LEFT JOIN treatments t ON t.specialty_id = s.id AND t.is_active = true
                LEFT JOIN cost_generated cg ON cg.treatment_id = t.id
                WHERE s.is_active = true
                GROUP BY s.id, s.name
                HAVING COUNT(DISTINCT t.id) > 0
                ORDER BY s.name
            """)
            return cur.fetchall()
    except Exception as e:
        print(f"[Coverage Error] {e}")
        return []
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# PROMPTS
# ═══════════════════════════════════════════════════════════════

def get_active_prompt(prompt_type):
    conn = get_conn()
    if not conn:
        if prompt_type == 'extraction':
            return {'prompt_text': DEFAULT_EXTRACTION_PROMPT}
        return {'prompt_text': DEFAULT_GENERATION_PROMPT}
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM cost_prompts
                WHERE prompt_type = %s AND is_active = true
                ORDER BY updated_at DESC LIMIT 1
            """, [prompt_type])
            result = cur.fetchone()
            if not result:
                if prompt_type == 'extraction':
                    return {'prompt_text': DEFAULT_EXTRACTION_PROMPT}
                return {'prompt_text': DEFAULT_GENERATION_PROMPT}
            return result
    except Exception as e:
        print(f"[Prompt Error] {e}")
        if prompt_type == 'extraction':
            return {'prompt_text': DEFAULT_EXTRACTION_PROMPT}
        return {'prompt_text': DEFAULT_GENERATION_PROMPT}
    finally:
        conn.close()


def get_all_prompts():
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM cost_prompts ORDER BY prompt_type, updated_at DESC")
            return cur.fetchall()
    except Exception as e:
        print(f"[Prompts Error] {e}")
        return []
    finally:
        conn.close()


def save_prompt(prompt_id, name, prompt_type, prompt_text, set_active, created_by):
    conn = get_conn()
    if not conn:
        return False
    try:
        with conn.cursor() as cur:
            if set_active:
                cur.execute("UPDATE cost_prompts SET is_active = false WHERE prompt_type = %s", [prompt_type])

            if prompt_id:
                cur.execute("""
                    UPDATE cost_prompts
                    SET name = %s, prompt_text = %s, is_active = %s, updated_at = NOW()
                    WHERE id = %s
                """, [name, prompt_text, set_active, prompt_id])
            else:
                cur.execute("""
                    INSERT INTO cost_prompts (name, prompt_type, prompt_text, is_active, created_by)
                    VALUES (%s, %s, %s, %s, %s)
                """, [name, prompt_type, prompt_text, set_active, created_by])
            conn.commit()
            return True
    except Exception as e:
        print(f"[Save Prompt Error] {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


# ═══════════════════════════════════════════════════════════════
# RECENT ACTIVITY
# ═══════════════════════════════════════════════════════════════

def get_recent_activity(limit=15):
    conn = get_conn()
    if not conn:
        return []
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                (SELECT 'generated' as activity_type, treatment_name as title,
                        status, generated_by as user_email, created_at
                 FROM cost_generated ORDER BY created_at DESC LIMIT %s)
                UNION ALL
                (SELECT 'ingested' as activity_type, source_name as title,
                        status, uploaded_by as user_email, created_at
                 FROM cost_sources ORDER BY created_at DESC LIMIT %s)
                ORDER BY created_at DESC LIMIT %s
            """, [limit, limit, limit])
            return cur.fetchall()
    except Exception as e:
        print(f"[Activity Error] {e}")
        return []
    finally:
        conn.close()
