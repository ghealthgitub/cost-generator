"""
🫚 Ginger Universe — Cost Engine
Claude-powered cost section generation and ad-hoc estimates
"""

import json
import config
import re

CLAUDE_AVAILABLE = False
try:
    from anthropic import Anthropic
    CLAUDE_AVAILABLE = bool(config.ANTHROPIC_API_KEY)
except ImportError:
    pass


def generate_cost_section(treatment, calibration_data, specialty_calibration, prompt_template):
    """
    Generate an HTML cost section for a treatment page.

    Args:
        treatment: dict with name, description, content (article text)
        calibration_data: list of real pricing entries for this treatment
        specialty_calibration: list of pricing entries for related treatments
        prompt_template: the active generation prompt
    Returns:
        dict with html, prompt_used, calibration_summary
    """
    if not CLAUDE_AVAILABLE:
        return {'error': 'Claude API not available'}

    # Build calibration context
    cal_lines = []
    if calibration_data:
        cal_lines.append("=== REAL PRICING DATA FOR THIS TREATMENT ===")
        for d in calibration_data:
            line = f"- {d.get('treatment_name_raw', 'N/A')} at {d.get('hospital_name_raw', 'Unknown')}, {d.get('city', '')}: "
            line += f"₹{d.get('total_cost_min', '?'):,} – ₹{d.get('total_cost_max', '?'):,}"
            if d.get('stay_days_min'):
                line += f" ({d['stay_days_min']}-{d.get('stay_days_max', d['stay_days_min'])} days)"
            if d.get('is_daycare'):
                line += " [Daycare]"
            # Add components if available
            comps = d.get('components')
            if comps and isinstance(comps, list):
                comp_parts = []
                for c in comps:
                    if isinstance(c, dict) and c.get('type'):
                        comp_parts.append(f"  • {c['type']}: ₹{c.get('amount_min', '?')} – ₹{c.get('amount_max', '?')}")
                if comp_parts:
                    line += "\n" + "\n".join(comp_parts)
            cal_lines.append(line)

    if specialty_calibration:
        cal_lines.append("\n=== PRICING DATA FOR RELATED TREATMENTS (same specialty) ===")
        for d in specialty_calibration[:30]:  # Limit context size
            line = f"- {d.get('treatment_name_raw', 'N/A')}: ₹{d.get('total_cost_min', '?'):,} – ₹{d.get('total_cost_max', '?'):,}"
            if d.get('city'):
                line += f" ({d['city']})"
            cal_lines.append(line)

    calibration_text = "\n".join(cal_lines) if cal_lines else "No real hospital pricing data available yet. Use your medical knowledge of Indian hospital costs to estimate."

    # Build article text (strip HTML tags for cleaner context)
    article_text = treatment.get('content', '') or treatment.get('description', '') or ''
    article_text = re.sub(r'<[^>]+>', ' ', article_text)  # Strip HTML
    article_text = re.sub(r'\s+', ' ', article_text).strip()
    if len(article_text) > 6000:
        article_text = article_text[:6000] + "..."

    # Compose final prompt
    final_prompt = f"""{prompt_template}

=== TREATMENT ARTICLE ===
Treatment: {treatment.get('name', 'Unknown')}
Specialty: {treatment.get('specialty_name', 'Unknown')}

{article_text}

=== CALIBRATION DATA (real hospital pricing) ===
{calibration_text}

Generate the HTML cost section now for: {treatment.get('name', 'Unknown')}"""

    try:
        client = Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{"role": "user", "content": final_prompt}]
        )
        html_output = message.content[0].text.strip()

        # Clean up if wrapped in code fences
        if html_output.startswith('```'):
            html_output = html_output.split('\n', 1)[1] if '\n' in html_output else html_output[3:]
        if html_output.endswith('```'):
            html_output = html_output[:-3]
        html_output = html_output.strip()
        if html_output.startswith('html'):
            html_output = html_output[4:].strip()

        return {
            'html': html_output,
            'prompt_used': final_prompt,
            'calibration_summary': calibration_text[:2000]
        }

    except Exception as e:
        print(f"[Cost Engine Error] {e}")
        return {'error': str(e)}


def quick_estimate(procedure_name, calibration_data_all):
    """
    Generate an ad-hoc cost estimate for any procedure name.
    Used by counselors for quick patient queries.

    Args:
        procedure_name: what the counselor typed
        calibration_data_all: summary of all available pricing data
    Returns:
        dict with html estimate and reasoning
    """
    if not CLAUDE_AVAILABLE:
        return {'error': 'Claude API not available'}

    # Build calibration summary
    cal_lines = []
    for d in calibration_data_all[:200]:  # Limit
        line = f"- {d.get('treatment_name_raw', 'N/A')}: ₹{d.get('total_cost_min', '?'):,} – ₹{d.get('total_cost_max', '?'):,}"
        if d.get('city'):
            line += f" ({d['city']})"
        if d.get('is_daycare'):
            line += " [Daycare]"
        cal_lines.append(line)

    calibration_text = "\n".join(cal_lines) if cal_lines else "No calibration data available."

    prompt = f"""You are a medical cost advisor for Ginger Healthcare, India's medical tourism platform.

A patient counselor needs a quick cost estimate for: "{procedure_name}"

Use the calibration data below (real Indian hospital pricing we've collected) to ground your estimate. If the exact procedure isn't in the data, reason from similar procedures and your medical knowledge about what this procedure involves.

=== CALIBRATION DATA (real Indian hospital pricing) ===
{calibration_text}

Return ONLY this HTML structure:

<div class="estimate-result">
  <h3>{procedure_name}</h3>
  <p class="estimate-range"><strong>Estimated Cost in India: ₹X,XX,XXX – ₹X,XX,XXX</strong></p>

  <p class="estimate-summary">[2-3 sentences: what this procedure involves and what drives the cost. Be specific to this procedure.]</p>

  <table class="cost-table">
    <thead><tr><th>Component</th><th>Estimated Range (₹)</th></tr></thead>
    <tbody>
      <tr><td>[Component]</td><td>₹XX,XXX – ₹XX,XXX</td></tr>
    </tbody>
    <tfoot>
      <tr><td><strong>Total Estimated</strong></td><td><strong>₹X,XX,XXX – ₹X,XX,XXX</strong></td></tr>
    </tfoot>
  </table>

  <p class="estimate-note">[1 sentence: what factors cause variation. Mention city, hospital tier, specific things like implant type if relevant.]</p>

  <p class="estimate-confidence"><em>Confidence: [High/Medium/Low] — [1 sentence explaining why: is this based on direct data, similar procedures, or general reasoning]</em></p>
</div>

Use Indian number formatting: ₹1,50,000 not ₹150,000. Return ONLY the HTML, no other text."""

    try:
        client = Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1500,
            messages=[{"role": "user", "content": prompt}]
        )
        html_output = message.content[0].text.strip()

        # Clean up code fences
        if html_output.startswith('```'):
            html_output = html_output.split('\n', 1)[1] if '\n' in html_output else html_output[3:]
        if html_output.endswith('```'):
            html_output = html_output[:-3]
        html_output = html_output.strip()
        if html_output.startswith('html'):
            html_output = html_output[4:].strip()

        return {
            'html': html_output,
            'procedure': procedure_name
        }

    except Exception as e:
        print(f"[Quick Estimate Error] {e}")
        return {'error': str(e)}


def extract_from_content(content_text, prompt_template, source_type='csv'):
    """
    Use Claude to extract structured pricing data from raw content.
    Used during data ingestion (CSV, scraped HTML, OCR text from images).

    Returns list of standardized cost entries.
    """
    if not CLAUDE_AVAILABLE:
        return {'error': 'Claude API not available'}

    final_prompt = f"""{prompt_template}

=== SOURCE TYPE: {source_type.upper()} ===

{content_text[:8000]}

Extract ALL treatment pricing data from the above content. Return a JSON array."""

    try:
        client = Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": final_prompt}]
        )
        raw = message.content[0].text.strip()

        # Clean JSON
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1] if '\n' in raw else raw[3:]
        if raw.endswith('```'):
            raw = raw[:-3]
        raw = raw.strip()
        if raw.startswith('json'):
            raw = raw[4:].strip()

        entries = json.loads(raw)
        if not isinstance(entries, list):
            entries = [entries]

        return {'entries': entries}

    except json.JSONDecodeError as e:
        print(f"[Extract JSON Error] {e}")
        return {'error': f'Could not parse Claude response as JSON: {e}'}
    except Exception as e:
        print(f"[Extract Error] {e}")
        return {'error': str(e)}


def extract_from_images(file_images, prompt_template):
    """
    Use Claude vision to extract pricing from uploaded PDF/image rate cards.

    Args:
        file_images: list of dicts with type, media_type, data (base64)
        prompt_template: extraction prompt
    Returns:
        dict with entries list
    """
    if not CLAUDE_AVAILABLE:
        return {'error': 'Claude API not available'}

    content_parts = []
    for img in file_images:
        content_parts.append({
            'type': img['type'],
            'source': {'type': 'base64', 'media_type': img['media_type'], 'data': img['data']}
        })
    content_parts.append({
        'type': 'text',
        'text': f"""{prompt_template}

The above image(s)/document(s) contain hospital rate cards or pricing information.
Extract ALL treatment pricing data. Return a JSON array.
Pay special attention to: treatment names, package costs, room types, stay duration, inclusions/exclusions.
Convert all amounts to INR if they aren't already."""
    })

    try:
        client = Anthropic(api_key=config.ANTHROPIC_API_KEY)
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=4000,
            messages=[{"role": "user", "content": content_parts}]
        )
        raw = message.content[0].text.strip()

        # Clean JSON
        if raw.startswith('```'):
            raw = raw.split('\n', 1)[1] if '\n' in raw else raw[3:]
        if raw.endswith('```'):
            raw = raw[:-3]
        raw = raw.strip()
        if raw.startswith('json'):
            raw = raw[4:].strip()

        entries = json.loads(raw)
        if not isinstance(entries, list):
            entries = [entries]

        return {'entries': entries}

    except json.JSONDecodeError as e:
        return {'error': f'Could not parse response as JSON: {e}'}
    except Exception as e:
        return {'error': str(e)}
