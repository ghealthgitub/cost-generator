"""
🫚 Ginger Universe — Cost Scraper
Scrapes hospital pricing pages for cost data extraction
"""

import requests
from bs4 import BeautifulSoup
import re


def scrape_pricing_urls(urls):
    """
    Scrape one or more hospital pricing/package URLs.
    Returns combined text content for Claude to extract pricing from.
    """
    results = []
    errors = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }

    for url in urls:
        url = url.strip()
        if not url:
            continue
        try:
            resp = requests.get(url, headers=headers, timeout=20, allow_redirects=True)
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, 'html.parser')

            # Remove scripts, styles, nav, footer
            for tag in soup(['script', 'style', 'nav', 'footer', 'header', 'aside']):
                tag.decompose()

            # Get title
            title = soup.title.string.strip() if soup.title and soup.title.string else url

            # Look for pricing-specific content: tables, price mentions
            text_parts = [f"=== Source: {url} ===", f"Title: {title}"]

            # Extract tables (often contain pricing)
            tables = soup.find_all('table')
            for i, table in enumerate(tables):
                text_parts.append(f"\n[Table {i+1}]")
                for row in table.find_all('tr'):
                    cells = [td.get_text(strip=True) for td in row.find_all(['td', 'th'])]
                    if any(cells):
                        text_parts.append(' | '.join(cells))

            # Get main body text
            body_text = soup.get_text(separator='\n', strip=True)
            # Clean up excessive whitespace
            body_text = re.sub(r'\n{3,}', '\n\n', body_text)
            body_text = re.sub(r' {2,}', ' ', body_text)

            text_parts.append(f"\n[Full Text]\n{body_text[:5000]}")
            results.append('\n'.join(text_parts))

        except Exception as e:
            errors.append(f"{url}: {str(e)}")
            print(f"[Scrape Error] {url}: {e}")

    combined = '\n\n'.join(results) if results else ''
    return {
        'text': combined,
        'url_count': len(results),
        'errors': errors,
        'total_chars': len(combined)
    }
