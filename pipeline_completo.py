"""
Pipeline Completo — ProspectAdv
Processamento em escala de advogados: CSV -> Google Custom Search -> Scrape -> Validate -> CSV

Entrada: CSV com colunas (nome, oab_num)
Saida:   CSV com colunas (nome, oab, cnpj, endereco, telefone_full, email, site,
                          has_website, valid_phone, valid_email)

Modulos usados:
- anti_detection.py: SessionManager, Selenium stealth, LGPD filtering
- validador_contatos.py: Phone/email/CNPJ validation
- prospectar_advogados.py: Domain brute-force, SEO extraction

Fluxo por registro:
1. Google Custom Search API (q="advogado" + nome + "telefone")
2. Se tem site nos resultados -> scrape email + fone (requests + BeautifulSoup / Selenium fallback)
3. Se NAO tem site -> Domain brute-force (.adv.br, .com.br)
4. CNPJ lookup via BrasilAPI
5. Validacao: formato telefone BR, MX email, WhatsApp check
6. Flag: contact_ok = (valid_phone OR valid_email)
7. Salvar no banco + exportar CSV

Anti-bloqueio:
- Max 5 requests concorrentes por IP
- Accept-Language pt-BR com proxy BR
- Queries de ruido entre consultas (15% ratio)
- Delay humano nao-uniforme (log-normal)
- Rotacao de User-Agent a cada 15-30 requests
"""

import os
import re
import csv
import json
import time
import random
import sqlite3
import logging
import argparse
from datetime import datetime
from urllib.parse import quote_plus, urlparse

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

from anti_detection import SessionManager, scrape_com_selenium, filtrar_dados_lgpd
from validador_contatos import (
    validar_telefone_br, validar_email_completo,
    validar_contato_completo, buscar_cnpj_por_nome, consultar_cnpj,
    limpar_telefone, formatar_telefone_br,
)
from prospectar_advogados import (
    verificar_site_completo, extrair_dados_seo,
    gerar_slugs_avancados, verificar_dominios_completo,
    _normalizar, _headers,
)

load_dotenv()

logger = logging.getLogger("ProspectAdv.Pipeline")

DATABASE = "prospeccao_adv.db"

# Configuracao via .env
GOOGLE_CSE_KEY = os.getenv("GOOGLE_CUSTOM_SEARCH_KEY", "")
GOOGLE_CSE_CX = os.getenv("GOOGLE_CUSTOM_SEARCH_CX", "")
WPP_API_URL = os.getenv("WPPCONNECT_URL", "")
WPP_SECRET = os.getenv("WPPCONNECT_SECRET_KEY", "")


# ============================================================
# 1. GOOGLE CUSTOM SEARCH API
# ============================================================

def google_custom_search(query, key=None, cx=None, num=10):
    """
    Busca via Google Custom Search API (programmatic, sem risco de bloqueio).
    100 queries/dia gratis, $5/1000 depois.

    Args:
        query: String de busca
        key: API key (ou usa GOOGLE_CUSTOM_SEARCH_KEY do .env)
        cx: Custom Search Engine ID (ou usa GOOGLE_CUSTOM_SEARCH_CX do .env)
        num: Numero de resultados (max 10 por request)

    Returns:
        list of dict: [{title, link, snippet}, ...]
    """
    api_key = key or GOOGLE_CSE_KEY
    search_cx = cx or GOOGLE_CSE_CX

    if not api_key or not search_cx:
        logger.warning("Google Custom Search API nao configurada (GOOGLE_CUSTOM_SEARCH_KEY / GOOGLE_CUSTOM_SEARCH_CX)")
        return None

    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": api_key,
        "cx": search_cx,
        "q": query,
        "num": min(num, 10),
        "lr": "lang_pt",
        "gl": "br",
        "hl": "pt-BR",
    }

    try:
        resp = requests.get(url, params=params, timeout=15)

        if resp.status_code == 429:
            logger.warning("Google CSE: quota excedida (429)")
            return None

        if resp.status_code != 200:
            logger.warning(f"Google CSE status {resp.status_code}: {resp.text[:200]}")
            return None

        data = resp.json()
        items = data.get("items", [])

        resultados = []
        for item in items:
            resultados.append({
                "title": item.get("title", ""),
                "link": item.get("link", ""),
                "snippet": item.get("snippet", ""),
                "displayLink": item.get("displayLink", ""),
            })

        return resultados

    except Exception as e:
        logger.error(f"Erro Google CSE: {e}")
        return None


def google_search_fallback(query, session_mgr=None):
    """
    Fallback: scraping do Google Search (sem API key).
    Usa anti-deteccao completa.

    Mais arriscado que CSE, mas gratuito e ilimitado.
    """
    if session_mgr is None:
        session_mgr = SessionManager()

    headers = session_mgr.get_headers(referer="https://www.google.com.br/")

    try:
        resp = requests.get(
            f"https://www.google.com.br/search?q={quote_plus(query)}&num=10&hl=pt-BR",
            headers=headers,
            timeout=12,
        )

        if resp.status_code != 200:
            return None

        soup = BeautifulSoup(resp.text, "html.parser")
        resultados = []

        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"]
            url = None
            if "/url?q=" in href:
                url = href.split("/url?q=")[1].split("&")[0]
            elif href.startswith("http") and "google" not in href:
                url = href

            if url:
                # Pegar texto do link como titulo
                title = a_tag.get_text(strip=True)[:100]
                resultados.append({
                    "title": title,
                    "link": url,
                    "snippet": "",
                    "displayLink": urlparse(url).netloc,
                })

        return resultados if resultados else None

    except Exception as e:
        logger.debug(f"Erro Google fallback: {e}")
        return None


# ============================================================
# 2. SCRAPING DE CONTATOS (Email + Telefone de sites)
# ============================================================

DOMINIOS_EXCLUIR = {
    "instagram.com", "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "jusbrasil.com.br", "escavador.com", "migalhas.com.br", "conjur.com.br",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "oab.org.br", "jus.br", "gov.br", "wikipedia.org",
    "reclameaqui.com.br", "glassdoor.com", "indeed.com",
    "maps.google.com", "goo.gl", "bit.ly",
    "apontador.com.br", "guiamais.com.br", "telelistas.net",
    "yelp.com", "tripadvisor.com",
}


def extrair_contatos_html(html, url=None):
    """
    Extrai telefones, emails, endereco e redes sociais do HTML.
    Usa regex otimizado + BeautifulSoup para Schema.org.
    """
    resultado = {
        "telefones": [],
        "emails": [],
        "endereco": None,
        "instagram": None,
        "facebook": None,
        "linkedin": None,
        "cnpj": None,
        "oab_mencionada": False,
        "areas_atuacao": [],
    }

    if not html:
        return resultado

    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(separator=" ", strip=True)

    # === TELEFONES ===
    patterns_tel = [
        r"\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}",
        r"\+55\s*\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}",
        r"(?:Tel|Fone|Telefone|Phone|Cel|Celular|WhatsApp)[:\s]*\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}",
    ]
    for pat in patterns_tel:
        matches = re.findall(pat, html, re.IGNORECASE)
        for m in matches:
            digitos = re.sub(r"\D", "", m)
            if 10 <= len(digitos) <= 13 and digitos not in [t["digitos"] for t in resultado["telefones"]]:
                resultado["telefones"].append({
                    "original": m.strip(),
                    "digitos": digitos,
                    "validacao": validar_telefone_br(digitos),
                })

    # === EMAILS ===
    emails_raw = re.findall(
        r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
        html,
    )
    filtro_excluir = [
        "google", "gstatic", "example", "sentry", "w3.org",
        "schema.org", "noreply", "wix", "wordpress", "jquery",
        "bootstrap", "fontawesome", "cloudflare", "gravatar",
        "facebook", "twitter", "instagram",
    ]
    for email in emails_raw:
        email_lower = email.lower()
        if not any(x in email_lower for x in filtro_excluir):
            if email_lower not in [e["email"] for e in resultado["emails"]]:
                resultado["emails"].append({
                    "email": email_lower,
                    "validacao": validar_email_completo(email_lower),
                })

    # === ENDERECO (heuristica) ===
    # Buscar em Schema.org primeiro
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            ld = json.loads(script.string)
            if isinstance(ld, dict):
                addr = ld.get("address", {})
                if isinstance(addr, dict):
                    parts = [
                        addr.get("streetAddress", ""),
                        addr.get("addressLocality", ""),
                        addr.get("addressRegion", ""),
                        addr.get("postalCode", ""),
                    ]
                    endereco = ", ".join(p for p in parts if p)
                    if endereco:
                        resultado["endereco"] = endereco
        except (json.JSONDecodeError, TypeError):
            pass

    # Fallback: regex para CEP + enderecos BR
    if not resultado["endereco"]:
        cep_match = re.search(r"\d{5}[-.]?\d{3}", text)
        if cep_match:
            # Pegar contexto ao redor do CEP
            idx = text.find(cep_match.group())
            start = max(0, idx - 150)
            end = min(len(text), idx + 50)
            contexto = text[start:end].strip()
            # Limpar
            contexto = re.sub(r"\s+", " ", contexto)
            resultado["endereco"] = contexto[:200]

    # === CNPJ ===
    cnpjs = re.findall(r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}", text)
    if cnpjs:
        resultado["cnpj"] = re.sub(r"\D", "", cnpjs[0])

    # === REDES SOCIAIS ===
    for link in soup.find_all("a", href=True):
        href = link["href"].lower()
        if "instagram.com/" in href:
            match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", href)
            if match and match.group(1) not in ("explore", "p", "reel", "stories", "accounts"):
                resultado["instagram"] = f"@{match.group(1)}"
        elif "facebook.com/" in href:
            match = re.search(r"facebook\.com/([a-zA-Z0-9_.]+)", href)
            if match and match.group(1) not in ("login", "pages", "groups", "sharer", "share"):
                resultado["facebook"] = match.group(1)
        elif "linkedin.com/" in href:
            match = re.search(r"linkedin\.com/(?:in|company)/([a-zA-Z0-9_-]+)", href)
            if match:
                resultado["linkedin"] = match.group(1)

    # === OAB ===
    text_lower = _normalizar(text)
    if re.search(r"oab[\s/]*[a-z]{2}[\s]*\d", text_lower):
        resultado["oab_mencionada"] = True

    # === AREAS DE ATUACAO ===
    MAPEAMENTO = {
        "trabalhist": "Direito Trabalhista",
        "criminal": "Direito Criminal",
        "civil": "Direito Civil",
        "consumidor": "Direito do Consumidor",
        "empresarial": "Direito Empresarial",
        "tributari": "Direito Tributario",
        "familia": "Direito de Familia",
        "previdenciari": "Direito Previdenciario",
        "imobiliari": "Direito Imobiliario",
        "ambiental": "Direito Ambiental",
        "digital": "Direito Digital",
    }
    for kw, area in MAPEAMENTO.items():
        if kw in text_lower and area not in resultado["areas_atuacao"]:
            resultado["areas_atuacao"].append(area)

    return resultado


def scrape_site(url, session_mgr=None, use_selenium=False, driver=None):
    """
    Raspa contatos de um site. Tenta requests primeiro, Selenium se falhar.
    """
    if session_mgr is None:
        session_mgr = SessionManager()

    html = None

    # Tentativa 1: requests (rapido)
    try:
        headers = session_mgr.get_headers()
        resp = requests.get(url, headers=headers, timeout=(5, 15), verify=False, allow_redirects=True)
        if resp.status_code < 400:
            html = resp.text
    except Exception as e:
        logger.debug(f"Requests falhou para {url}: {e}")

    # Tentativa 2: Selenium (se habilitado e requests falhou)
    if not html and use_selenium:
        logger.debug(f"Tentando Selenium para {url}")
        html = scrape_com_selenium(url, driver=driver)

    if not html:
        return None

    contatos = extrair_contatos_html(html, url)

    # Tambem extrair dados SEO
    try:
        seo = extrair_dados_seo(url)
        contatos["titulo_site"] = seo.get("titulo", "")
        contatos["descricao_site"] = seo.get("descricao", "")
    except Exception:
        contatos["titulo_site"] = ""
        contatos["descricao_site"] = ""

    return contatos


# ============================================================
# 3. PIPELINE PRINCIPAL
# ============================================================

def processar_advogado(nome, oab_num, session_mgr, batch_stats, use_selenium=False, driver=None):
    """
    Processa UM advogado no pipeline completo.

    Fluxo:
    1. Google Custom Search: "advogado" + nome + "telefone"
    2. Se site nos resultados -> scrape contatos
    3. Se nao -> domain brute-force (.adv.br/.com.br)
    4. CNPJ lookup
    5. Validacao de contatos
    6. Retorna dict pronto para CSV

    Returns:
        dict com todas as colunas do CSV de saida
    """
    logger.info(f"--- Processando: {nome} (OAB: {oab_num}) ---")

    resultado = {
        "nome": nome,
        "oab": oab_num or "",
        "cnpj": "",
        "endereco": "",
        "telefone_full": "",
        "email": "",
        "site": "",
        "has_website": 0,
        "valid_phone": 0,
        "valid_email": 0,
        "instagram": "",
        "facebook": "",
        "linkedin": "",
        "areas_atuacao": "",
        "titulo_site": "",
        "fonte": "",
        "processado_em": datetime.now().isoformat(),
    }

    site_url = None
    contatos_scrape = None

    # ============================================
    # ETAPA 1: Google Custom Search API
    # ============================================
    logger.info(f"  [1/5] Google Custom Search...")

    queries = [
        f'"advogado" "{nome}" "telefone"',
        f'"{nome}" advogado OAB',
        f'"{nome}" escritorio advocacia',
    ]

    google_results = None
    for query in queries:
        # Ruido entre queries
        if session_mgr.should_noise():
            session_mgr.execute_noise()

        # Tentar CSE API primeiro (se configurada)
        if GOOGLE_CSE_KEY and GOOGLE_CSE_CX:
            google_results = google_custom_search(query)
        else:
            # Fallback: scraping (com anti-deteccao)
            google_results = google_search_fallback(query, session_mgr)

        if google_results:
            break

        session_mgr.human_delay(1.0, 2.5)

    # Analisar resultados do Google
    if google_results:
        for item in google_results:
            link = item.get("link", "")
            display = item.get("displayLink", "")

            # Ignorar redes sociais e agregadores
            if any(excl in display.lower() for excl in DOMINIOS_EXCLUIR):
                continue

            # Site encontrado!
            if link and not site_url:
                site_url = link
                resultado["fonte"] = "google_custom_search"
                logger.info(f"  Site encontrado via Google: {site_url}")
                break

    # ============================================
    # ETAPA 2: Se tem site -> scrape contatos
    # ============================================
    if site_url:
        logger.info(f"  [2/5] Scraping contatos de {site_url}...")
        resultado["has_website"] = 1
        resultado["site"] = site_url

        session_mgr.human_delay(0.5, 1.5)

        contatos_scrape = scrape_site(site_url, session_mgr, use_selenium, driver)

        if contatos_scrape:
            # Melhor telefone
            if contatos_scrape.get("telefones"):
                # Priorizar celular valido
                for tel in contatos_scrape["telefones"]:
                    if tel["validacao"]["valido"]:
                        resultado["telefone_full"] = tel["validacao"]["numero_full"]
                        break
                if not resultado["telefone_full"] and contatos_scrape["telefones"]:
                    resultado["telefone_full"] = contatos_scrape["telefones"][0]["digitos"]

            # Melhor email
            if contatos_scrape.get("emails"):
                for em in contatos_scrape["emails"]:
                    if em["validacao"]["valido"]:
                        resultado["email"] = em["email"]
                        break
                if not resultado["email"] and contatos_scrape["emails"]:
                    resultado["email"] = contatos_scrape["emails"][0]["email"]

            # Endereco
            if contatos_scrape.get("endereco"):
                resultado["endereco"] = contatos_scrape["endereco"]

            # CNPJ do site
            if contatos_scrape.get("cnpj"):
                resultado["cnpj"] = contatos_scrape["cnpj"]

            # Redes sociais
            for rede in ("instagram", "facebook", "linkedin"):
                if contatos_scrape.get(rede):
                    resultado[rede] = contatos_scrape[rede]

            # Areas
            if contatos_scrape.get("areas_atuacao"):
                resultado["areas_atuacao"] = json.dumps(contatos_scrape["areas_atuacao"])

            # Titulo
            if contatos_scrape.get("titulo_site"):
                resultado["titulo_site"] = contatos_scrape["titulo_site"]

    # ============================================
    # ETAPA 3: Se NAO tem site -> domain brute-force
    # ============================================
    if not site_url:
        logger.info(f"  [2/5] Sem site no Google. Domain brute-force...")

        # Inferir nome do escritorio a partir do nome do advogado
        partes_nome = nome.split()
        sobrenome = partes_nome[-1] if partes_nome else nome
        for prefix in ["Dr.", "Dra.", "Dr", "Dra"]:
            sobrenome = sobrenome.replace(prefix, "").strip()

        nome_escritorio = f"{sobrenome} Advogados"

        verificacao = verificar_site_completo(nome, nome_escritorio)

        if verificacao["tem_site"]:
            site_url = verificacao["site_url"]
            resultado["has_website"] = 1
            resultado["site"] = site_url
            resultado["fonte"] = f"brute_force_{verificacao.get('fonte_deteccao', '')}"
            logger.info(f"  Site encontrado via brute-force: {site_url}")

            # Scrape contatos do site encontrado
            session_mgr.human_delay(0.5, 1.5)
            contatos_scrape = scrape_site(site_url, session_mgr, use_selenium, driver)
            if contatos_scrape:
                if contatos_scrape.get("telefones"):
                    for tel in contatos_scrape["telefones"]:
                        if tel["validacao"]["valido"]:
                            resultado["telefone_full"] = tel["validacao"]["numero_full"]
                            break
                if contatos_scrape.get("emails"):
                    for em in contatos_scrape["emails"]:
                        if em["validacao"]["valido"]:
                            resultado["email"] = em["email"]
                            break
                if contatos_scrape.get("endereco"):
                    resultado["endereco"] = contatos_scrape["endereco"]
                if contatos_scrape.get("cnpj"):
                    resultado["cnpj"] = contatos_scrape["cnpj"]
                for rede in ("instagram", "facebook", "linkedin"):
                    if contatos_scrape.get(rede):
                        resultado[rede] = contatos_scrape[rede]
                if contatos_scrape.get("areas_atuacao"):
                    resultado["areas_atuacao"] = json.dumps(contatos_scrape["areas_atuacao"])
        else:
            resultado["has_website"] = 0
            resultado["fonte"] = "sem_site_confirmado"
            logger.info(f"  Sem site confirmado (oportunidade de prospeccao)")

    # ============================================
    # ETAPA 4: CNPJ lookup
    # ============================================
    if not resultado["cnpj"]:
        logger.info(f"  [3/5] Buscando CNPJ...")
        session_mgr.human_delay(0.5, 1.0)

        cnpj_data = buscar_cnpj_por_nome(nome)
        if cnpj_data:
            resultado["cnpj"] = cnpj_data.get("cnpj_formatado", "")
            if not resultado["endereco"] and cnpj_data.get("endereco"):
                resultado["endereco"] = cnpj_data["endereco"]
            if not resultado["telefone_full"] and cnpj_data.get("telefone"):
                resultado["telefone_full"] = cnpj_data["telefone"]
            if not resultado["email"] and cnpj_data.get("email"):
                resultado["email"] = cnpj_data["email"]
            logger.info(f"  CNPJ encontrado: {resultado['cnpj']}")
        else:
            logger.debug(f"  CNPJ nao encontrado")

    # ============================================
    # ETAPA 5: Validacao de contatos
    # ============================================
    logger.info(f"  [4/5] Validando contatos...")

    validacao = validar_contato_completo(
        telefone=resultado["telefone_full"],
        email=resultado["email"],
        wpp_api_url=WPP_API_URL if WPP_API_URL else None,
        wpp_secret=WPP_SECRET if WPP_SECRET else None,
    )

    resultado["valid_phone"] = validacao["valid_phone"]
    resultado["valid_email"] = validacao["valid_email"]

    if validacao["telefone_formatado"]:
        resultado["telefone_full"] = validacao["telefone_formatado"]

    # contact_ok (pelo menos 1 canal valido)
    contact_ok = validacao["contact_ok"]

    logger.info(f"  [5/5] Resultado: site={'SIM' if resultado['has_website'] else 'NAO'} "
                f"tel={'OK' if resultado['valid_phone'] else 'X'} "
                f"email={'OK' if resultado['valid_email'] else 'X'} "
                f"contato={'OK' if contact_ok else 'X'}")

    # Atualizar stats
    batch_stats["processados"] += 1
    if resultado["has_website"]:
        batch_stats["com_site"] += 1
    if resultado["valid_phone"]:
        batch_stats["telefone_valido"] += 1
    if resultado["valid_email"]:
        batch_stats["email_valido"] += 1
    if contact_ok:
        batch_stats["contact_ok"] += 1

    return resultado


# ============================================================
# 4. PIPELINE BATCH (processar CSV inteiro)
# ============================================================

def executar_pipeline(
    csv_entrada,
    csv_saida=None,
    max_registros=None,
    use_selenium=False,
    salvar_banco=True,
    noise_ratio=0.15,
    base_delay=2.0,
):
    """
    Executa pipeline completo: CSV entrada -> processamento -> CSV saida.

    Args:
        csv_entrada: Caminho do CSV (colunas: nome, oab_num)
        csv_saida: Caminho do CSV de saida (default: output_YYYYMMDD.csv)
        max_registros: Limite de registros a processar (None = todos)
        use_selenium: Usar Selenium para sites que precisam JS
        salvar_banco: Salvar resultados no SQLite
        noise_ratio: Proporcao de queries de ruido (0.15 = 15%)
        base_delay: Delay base entre requests
    """
    if not csv_saida:
        csv_saida = f"output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    logger.info("\n" + "=" * 70)
    logger.info("PIPELINE COMPLETO — ProspectAdv")
    logger.info("=" * 70)
    logger.info(f"Entrada: {csv_entrada}")
    logger.info(f"Saida:   {csv_saida}")
    logger.info(f"Selenium: {'SIM' if use_selenium else 'NAO'}")
    logger.info(f"Noise ratio: {noise_ratio:.0%}")
    logger.info(f"Google CSE: {'CONFIGURADO' if GOOGLE_CSE_KEY else 'NAO (usando fallback)'}")
    logger.info("=" * 70)

    # Ler CSV de entrada
    registros = []
    try:
        with open(csv_entrada, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Aceitar variantes de nome de coluna
                nome = row.get("nome") or row.get("Nome") or row.get("NOME") or ""
                oab = row.get("oab_num") or row.get("oab") or row.get("OAB") or row.get("numero_oab") or ""
                if nome.strip():
                    registros.append({"nome": nome.strip(), "oab_num": oab.strip()})
    except FileNotFoundError:
        logger.error(f"Arquivo nao encontrado: {csv_entrada}")
        return None
    except Exception as e:
        logger.error(f"Erro lendo CSV: {e}")
        return None

    if max_registros:
        registros = registros[:max_registros]

    total = len(registros)
    logger.info(f"Total de registros: {total}")

    if total == 0:
        logger.warning("Nenhum registro para processar")
        return None

    # Iniciar sessao anti-deteccao
    session_mgr = SessionManager(
        max_concurrent=5,
        base_delay=base_delay,
        noise_ratio=noise_ratio,
    )

    # Selenium driver (reutilizar para eficiencia)
    driver = None
    if use_selenium:
        from anti_detection import criar_driver_stealth
        driver = criar_driver_stealth(headless=True)
        if driver:
            logger.info("Selenium WebDriver iniciado")
        else:
            logger.warning("Selenium nao disponivel, usando apenas requests")
            use_selenium = False

    # Stats do batch
    batch_stats = {
        "total": total,
        "processados": 0,
        "com_site": 0,
        "telefone_valido": 0,
        "email_valido": 0,
        "contact_ok": 0,
        "erros": 0,
        "inicio": datetime.now().isoformat(),
    }

    # Processar cada registro
    resultados = []
    colunas_csv = [
        "nome", "oab", "cnpj", "endereco", "telefone_full", "email",
        "site", "has_website", "valid_phone", "valid_email",
        "instagram", "facebook", "linkedin", "areas_atuacao",
        "titulo_site", "fonte", "processado_em",
    ]

    # Abrir CSV de saida (escrever header + resultados incrementais)
    with open(csv_saida, "w", newline="", encoding="utf-8-sig") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=colunas_csv, extrasaction="ignore")
        writer.writeheader()

        for i, reg in enumerate(registros, 1):
            logger.info(f"\n[{i}/{total}] ============================================")

            try:
                resultado = processar_advogado(
                    nome=reg["nome"],
                    oab_num=reg["oab_num"],
                    session_mgr=session_mgr,
                    batch_stats=batch_stats,
                    use_selenium=use_selenium,
                    driver=driver,
                )

                # Filtrar dados LGPD
                resultado = filtrar_dados_lgpd(resultado)

                resultados.append(resultado)

                # Escrever no CSV incrementalmente
                writer.writerow(resultado)
                f_out.flush()

                # Salvar no banco
                if salvar_banco:
                    _salvar_no_banco(resultado)

            except KeyboardInterrupt:
                logger.warning("\nInterrompido pelo usuario!")
                break
            except Exception as e:
                logger.error(f"Erro processando {reg['nome']}: {e}")
                batch_stats["erros"] += 1
                # Salvar registro com erro
                resultado_erro = {
                    "nome": reg["nome"],
                    "oab": reg["oab_num"],
                    "fonte": f"erro: {str(e)[:50]}",
                    "processado_em": datetime.now().isoformat(),
                }
                writer.writerow(resultado_erro)
                f_out.flush()

            # Delay humano entre registros
            if i < total:
                session_mgr.human_delay(1.5, 3.0)

                # Ruido periodico
                if session_mgr.should_noise():
                    session_mgr.execute_noise()

            # Progress report a cada 50 registros
            if i % 50 == 0:
                stats = session_mgr.stats()
                logger.info(f"\n--- PROGRESSO: {i}/{total} ({i/total*100:.0f}%) ---")
                logger.info(f"  Requests: {stats['requests']} ({stats['requests_per_minute']}/min)")
                logger.info(f"  Ruido: {stats['noise_queries']} queries")
                logger.info(f"  Com site: {batch_stats['com_site']}")
                logger.info(f"  Tel valido: {batch_stats['telefone_valido']}")
                logger.info(f"  Email valido: {batch_stats['email_valido']}")
                logger.info(f"  Contato OK: {batch_stats['contact_ok']}")

    # Fechar Selenium
    if driver:
        try:
            driver.quit()
        except Exception:
            pass

    # Relatorio final
    batch_stats["fim"] = datetime.now().isoformat()
    batch_stats["csv_saida"] = csv_saida
    stats = session_mgr.stats()

    logger.info("\n" + "=" * 70)
    logger.info("RELATORIO FINAL")
    logger.info("=" * 70)
    logger.info(f"Total processados: {batch_stats['processados']}/{total}")
    logger.info(f"Com site: {batch_stats['com_site']} ({batch_stats['com_site']/max(batch_stats['processados'],1)*100:.0f}%)")
    logger.info(f"Telefone valido: {batch_stats['telefone_valido']} ({batch_stats['telefone_valido']/max(batch_stats['processados'],1)*100:.0f}%)")
    logger.info(f"Email valido: {batch_stats['email_valido']} ({batch_stats['email_valido']/max(batch_stats['processados'],1)*100:.0f}%)")
    logger.info(f"Contato OK: {batch_stats['contact_ok']} ({batch_stats['contact_ok']/max(batch_stats['processados'],1)*100:.0f}%)")
    logger.info(f"Erros: {batch_stats['erros']}")
    logger.info(f"Requests totais: {stats['requests']}")
    logger.info(f"Queries de ruido: {stats['noise_queries']}")
    logger.info(f"Tempo: {stats['elapsed_seconds']}s ({stats['requests_per_minute']} req/min)")
    logger.info(f"CSV salvo: {csv_saida}")
    logger.info("=" * 70)

    return {
        "stats": batch_stats,
        "session_stats": stats,
        "resultados": resultados,
        "csv_saida": csv_saida,
    }


def _salvar_no_banco(resultado):
    """Salva resultado no SQLite (tabela advogados)."""
    db = sqlite3.connect(DATABASE)
    try:
        # Verificar duplicata
        existe = db.execute(
            "SELECT id FROM advogados WHERE nome = ?",
            (resultado.get("nome", ""),),
        ).fetchone()

        if existe:
            # Atualizar registro existente
            db.execute("""
                UPDATE advogados SET
                    numero_oab = COALESCE(NULLIF(?, ''), numero_oab),
                    email = COALESCE(NULLIF(?, ''), email),
                    telefone = COALESCE(NULLIF(?, ''), telefone),
                    endereco = COALESCE(NULLIF(?, ''), endereco),
                    tem_site = ?,
                    site_url = COALESCE(NULLIF(?, ''), site_url),
                    instagram = COALESCE(NULLIF(?, ''), instagram),
                    facebook = COALESCE(NULLIF(?, ''), facebook),
                    linkedin = COALESCE(NULLIF(?, ''), linkedin),
                    areas_atuacao = COALESCE(NULLIF(?, ''), areas_atuacao),
                    fonte = ?
                WHERE id = ?
            """, (
                resultado.get("oab", ""),
                resultado.get("email", ""),
                resultado.get("telefone_full", ""),
                resultado.get("endereco", ""),
                resultado.get("has_website", 0),
                resultado.get("site", ""),
                resultado.get("instagram", ""),
                resultado.get("facebook", ""),
                resultado.get("linkedin", ""),
                resultado.get("areas_atuacao", ""),
                resultado.get("fonte", ""),
                existe[0],
            ))
        else:
            # Inserir novo
            db.execute("""
                INSERT INTO advogados (
                    nome, numero_oab, email, telefone, endereco,
                    tem_site, site_url, instagram, facebook, linkedin,
                    areas_atuacao, fonte, score_potencial, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'novo')
            """, (
                resultado.get("nome", ""),
                resultado.get("oab", ""),
                resultado.get("email", ""),
                resultado.get("telefone_full", ""),
                resultado.get("endereco", ""),
                resultado.get("has_website", 0),
                resultado.get("site", ""),
                resultado.get("instagram", ""),
                resultado.get("facebook", ""),
                resultado.get("linkedin", ""),
                resultado.get("areas_atuacao", ""),
                resultado.get("fonte", "pipeline_completo"),
                35 if not resultado.get("has_website") else 10,
            ))

        db.commit()
    except Exception as e:
        logger.error(f"Erro salvando no banco: {e}")
    finally:
        db.close()


# ============================================================
# 5. GERAR CSV DE EXEMPLO (OAB-PR)
# ============================================================

def gerar_csv_exemplo(arquivo="entrada_oab_pr.csv", n=20):
    """
    Gera CSV de exemplo com nomes realistas de advogados do PR.
    Para testes do pipeline.
    """
    import random

    nomes_masculinos = [
        "Carlos Eduardo", "Paulo Henrique", "Roberto", "Fernando",
        "Ricardo", "Marcos Antonio", "Jose Carlos", "Andre",
        "Luiz Fernando", "Rafael", "Guilherme", "Pedro Augusto",
        "Thiago", "Bruno", "Leonardo", "Marcelo", "Eduardo",
        "Alexandre", "Flavio", "Luciano",
    ]
    nomes_femininos = [
        "Ana Beatriz", "Maria Fernanda", "Juliana", "Patricia",
        "Camila", "Daniela", "Luciana", "Adriana",
        "Renata", "Fernanda", "Cristiane", "Simone",
        "Vanessa", "Tatiana", "Carolina", "Amanda",
        "Mariana", "Larissa", "Isabela", "Leticia",
    ]
    sobrenomes = [
        "Silva", "Santos", "Oliveira", "Souza", "Rodrigues",
        "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
        "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida",
        "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
        "Rocha", "Dias", "Andrade", "Moreira", "Nunes",
        "Marques", "Machado", "Mendes", "Freitas", "Cardoso",
        "Ramos", "Teixeira", "Moura", "Correia", "Pinto",
        "Cunha", "Monteiro", "Borges", "Melo", "Azevedo",
    ]

    registros = []
    nomes_usados = set()

    while len(registros) < n:
        if random.random() < 0.5:
            primeiro = random.choice(nomes_masculinos)
        else:
            primeiro = random.choice(nomes_femininos)

        # 1 ou 2 sobrenomes
        if random.random() < 0.4:
            sobrenome = f"{random.choice(sobrenomes)} {random.choice(sobrenomes)}"
        else:
            sobrenome = random.choice(sobrenomes)

        nome_completo = f"{primeiro} {sobrenome}"
        if nome_completo in nomes_usados:
            continue
        nomes_usados.add(nome_completo)

        oab_num = f"{random.randint(10000, 99999)}"

        registros.append({"nome": nome_completo, "oab_num": oab_num})

    with open(arquivo, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=["nome", "oab_num"])
        writer.writeheader()
        writer.writerows(registros)

    logger.info(f"CSV de exemplo gerado: {arquivo} ({n} registros)")
    return arquivo


# ============================================================
# 6. MIGRAR SCHEMA DO BANCO (adicionar colunas novas)
# ============================================================

def migrar_banco():
    """Adiciona colunas novas ao banco se nao existirem."""
    db = sqlite3.connect(DATABASE)

    # Pegar colunas existentes
    cursor = db.execute("PRAGMA table_info(advogados)")
    colunas_existentes = {row[1] for row in cursor.fetchall()}

    novas_colunas = {
        "cnpj": "TEXT",
        "valid_phone": "INTEGER DEFAULT 0",
        "valid_email": "INTEGER DEFAULT 0",
        "contact_ok": "INTEGER DEFAULT 0",
    }

    for coluna, tipo in novas_colunas.items():
        if coluna not in colunas_existentes:
            try:
                db.execute(f"ALTER TABLE advogados ADD COLUMN {coluna} {tipo}")
                logger.info(f"  Coluna adicionada: {coluna} ({tipo})")
            except Exception as e:
                logger.debug(f"  Coluna {coluna} ja existe ou erro: {e}")

    db.commit()
    db.close()
    logger.info("Migracao do banco concluida")


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    parser = argparse.ArgumentParser(
        description="ProspectAdv Pipeline Completo — CSV -> Google -> Scrape -> Validate -> CSV"
    )
    parser.add_argument(
        "comando",
        choices=["processar", "exemplo", "migrar", "teste"],
        help="Comando: processar (CSV), exemplo (gerar CSV teste), migrar (DB schema), teste (5 nomes)"
    )
    parser.add_argument(
        "--entrada", "-i",
        default="entrada_oab_pr.csv",
        help="CSV de entrada (default: entrada_oab_pr.csv)"
    )
    parser.add_argument(
        "--saida", "-o",
        default=None,
        help="CSV de saida (default: output_YYYYMMDD.csv)"
    )
    parser.add_argument(
        "--max", "-n",
        type=int, default=None,
        help="Limite de registros a processar"
    )
    parser.add_argument(
        "--selenium",
        action="store_true",
        help="Usar Selenium para sites que precisam JS"
    )
    parser.add_argument(
        "--noise",
        type=float, default=0.15,
        help="Proporcao de queries de ruido (default: 0.15)"
    )
    parser.add_argument(
        "--delay",
        type=float, default=2.0,
        help="Delay base entre requests em segundos (default: 2.0)"
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Nao salvar no banco SQLite"
    )

    args = parser.parse_args()

    if args.comando == "exemplo":
        n = args.max or 20
        gerar_csv_exemplo(args.entrada, n)
        print(f"\nCSV de exemplo gerado: {args.entrada} ({n} registros)")
        print(f"Agora execute: python pipeline_completo.py processar --entrada {args.entrada}")

    elif args.comando == "migrar":
        print("Migrando schema do banco...")
        migrar_banco()
        print("Migracao concluida!")

    elif args.comando == "teste":
        print("\nGerando CSV de teste com 5 nomes...")
        gerar_csv_exemplo("_teste_pipeline.csv", 5)
        print("Executando pipeline de teste...\n")
        migrar_banco()
        resultado = executar_pipeline(
            csv_entrada="_teste_pipeline.csv",
            csv_saida="_teste_output.csv",
            max_registros=5,
            use_selenium=args.selenium,
            salvar_banco=not args.no_db,
            noise_ratio=0.0,  # Sem ruido no teste
            base_delay=1.0,   # Mais rapido no teste
        )
        if resultado:
            print(f"\nResultado salvo: _teste_output.csv")
            print(f"Stats: {json.dumps(resultado['stats'], indent=2)}")

    elif args.comando == "processar":
        print(f"\nProcessando: {args.entrada}")
        migrar_banco()
        resultado = executar_pipeline(
            csv_entrada=args.entrada,
            csv_saida=args.saida,
            max_registros=args.max,
            use_selenium=args.selenium,
            salvar_banco=not args.no_db,
            noise_ratio=args.noise,
            base_delay=args.delay,
        )
        if resultado:
            print(f"\nPipeline concluido! CSV: {resultado['csv_saida']}")
