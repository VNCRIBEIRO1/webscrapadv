"""
Enriquecer Advogados v2 — Sistema Completo de Enriquecimento e Validacao
ProspectAdv

Pipeline de enriquecimento:
1. Busca de site (.adv.br, Google Search, validacao HTTP)
2. Busca/validacao de redes sociais (Instagram, Facebook, LinkedIn)
3. Verificacao OAB CNA
4. Inferencia de dados (areas, porte)
5. Busca de email
6. Cruzamento de fontes
7. Recalculo de score
"""

import os
import re
import json
import time
import socket
import sqlite3
import logging
import random
import unicodedata
import requests
from datetime import datetime
from urllib.parse import quote_plus, urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger("ProspectAdv.Enriquecer")

DATABASE = "prospeccao_adv.db"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
]

# Dominios que NAO sao sites pessoais/escritorio
DOMINIOS_EXCLUIR = [
    "instagram.com", "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "jusbrasil.com", "escavador.com", "migalhas.com", "conjur.com",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "oab.org.br", "jus.br", "gov.br", "wikipedia.org",
    "reclameaqui.com", "glassdoor.com", "indeed.com",
    "example.com", "exemplo.com",
]


# ============================================================
# Utilitarios
# ============================================================

def _get_db():
    """Retorna conexao SQLite."""
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    return db


def _normalizar(texto):
    """Remove acentos e converte para minusculo."""
    if not texto:
        return ""
    nfkd = unicodedata.normalize("NFKD", str(texto))
    ascii_text = nfkd.encode("ASCII", "ignore").decode("ASCII")
    return ascii_text.lower().strip()


def _gerar_slug(texto):
    """Converte texto para slug alfanumerico."""
    text = _normalizar(texto)
    return re.sub(r"[^a-z0-9]", "", text)


def _headers():
    """Retorna headers HTTP com User-Agent aleatorio."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
    }


def _gerar_slugs_escritorio(nome_escritorio, nome=None):
    """Gera possiveis slugs de dominio a partir do nome do escritorio/advogado."""
    slugs = []

    if nome_escritorio:
        clean = _normalizar(nome_escritorio)
        # Remover sufixos comuns
        for suffix in [
            "advogados associados", "advogados", "advocacia",
            "associados", "escritorio de advocacia", "escritorio",
            "consultoria juridica", "assessoria juridica",
        ]:
            clean = clean.replace(suffix, "")
        # Remover conectores
        clean = re.sub(r"\b(e|de|do|da|dos|das)\b", "", clean)
        clean = clean.replace("&", "").strip()
        clean = re.sub(r"\s+", " ", clean).strip()

        if clean and len(clean) >= 2:
            # Slug solido: "costasantos"
            solid = re.sub(r"[^a-z0-9]", "", clean)
            if solid and len(solid) >= 3:
                slugs.append(solid)

            # Slug com hifen: "costa-santos"
            dashed = re.sub(r"\s+", "-", clean)
            dashed = re.sub(r"[^a-z0-9-]", "", dashed)
            dashed = re.sub(r"-+", "-", dashed).strip("-")
            if dashed and dashed not in slugs and len(dashed) >= 3:
                slugs.append(dashed)

        # Nome completo do escritorio como slug
        full_slug = _gerar_slug(nome_escritorio)
        if full_slug and full_slug not in slugs and len(full_slug) >= 4:
            slugs.append(full_slug)

    if nome:
        clean_name = _normalizar(nome)
        for prefix in ["dr. ", "dra. ", "dr ", "dra "]:
            clean_name = clean_name.replace(prefix, "")
        clean_name = clean_name.strip()

        parts = clean_name.split()
        if parts:
            # Sobrenome: "mendes"
            last = re.sub(r"[^a-z]", "", parts[-1])
            if last and len(last) >= 3 and last not in slugs:
                slugs.append(last)

            # Primeiro + Ultimo: "carlosmendes"
            if len(parts) >= 2:
                first_last = re.sub(r"[^a-z]", "", parts[0]) + last
                if first_last and first_last not in slugs:
                    slugs.append(first_last)

            # Nome completo sem espacos
            full_name = re.sub(r"[^a-z]", "", clean_name)
            if full_name and full_name not in slugs and len(full_name) >= 5:
                slugs.append(full_name)

    return slugs


# ============================================================
# Validacao de URL / Dominio
# ============================================================

def _dns_resolve(dominio):
    """Verifica se o dominio resolve via DNS."""
    try:
        socket.setdefaulttimeout(5)
        socket.getaddrinfo(dominio, 443, socket.AF_UNSPEC, socket.SOCK_STREAM)
        return True
    except (socket.gaierror, socket.timeout, OSError):
        return False


def validar_url(url, timeout=8):
    """
    Verifica se URL esta ativa e acessivel.
    Retorna a URL final (apos redirects) ou None.
    """
    try:
        parsed = urlparse(url)
        dominio = parsed.hostname
        if not dominio:
            return None

        # DNS check rapido
        if not _dns_resolve(dominio):
            return None

        # HEAD request
        resp = requests.head(
            url, timeout=timeout, allow_redirects=True,
            headers=_headers(), verify=False,
        )
        if resp.status_code < 400:
            return resp.url

        # Fallback: GET (alguns servers nao suportam HEAD)
        resp = requests.get(
            url, timeout=timeout, allow_redirects=True,
            headers=_headers(), stream=True, verify=False,
        )
        if resp.status_code < 400:
            return resp.url

    except (requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.TooManyRedirects):
        pass
    except Exception as e:
        logger.debug(f"Erro validando URL {url}: {e}")
    return None


def verificar_conteudo_site(url, nome, nome_escritorio=None):
    """
    Verifica se o conteudo do site menciona o advogado/escritorio.
    Retorna nivel de confianca (0.0 a 1.0).
    """
    try:
        resp = requests.get(
            url, timeout=12, headers=_headers(),
            verify=False, allow_redirects=True,
        )
        if resp.status_code >= 400:
            return 0.0

        # Normalizar HTML para comparacao
        soup = BeautifulSoup(resp.text, "html.parser")
        # Texto visivel
        text = _normalizar(soup.get_text(separator=" ", strip=True))
        title = _normalizar(soup.title.string if soup.title else "")

        confianca = 0.0

        # Verificar nome do escritorio no titulo
        if nome_escritorio:
            escritorio_norm = _normalizar(nome_escritorio)
            if escritorio_norm in title:
                confianca = max(confianca, 0.95)
            elif escritorio_norm in text:
                confianca = max(confianca, 0.85)
            # Verificar partes do nome
            partes = [p for p in escritorio_norm.split() if len(p) >= 3]
            matches = sum(1 for p in partes if p in text)
            if partes and matches >= len(partes) * 0.6:
                confianca = max(confianca, 0.75)

        # Verificar nome do advogado
        nome_norm = _normalizar(nome)
        # Remover Dr./Dra.
        for prefix in ["dr. ", "dra. ", "dr ", "dra "]:
            nome_norm = nome_norm.replace(prefix, "")
        nome_norm = nome_norm.strip()

        if nome_norm in title:
            confianca = max(confianca, 0.90)
        elif nome_norm in text:
            confianca = max(confianca, 0.80)

        # Verificar keywords juridicas
        keywords = ["advogad", "advocacia", "oab", "escritorio", "juridic", "direito"]
        keyword_hits = sum(1 for kw in keywords if kw in text)
        if keyword_hits >= 3:
            confianca = max(confianca, 0.5)
        elif keyword_hits >= 2:
            confianca = max(confianca, 0.35)

        return confianca

    except Exception as e:
        logger.debug(f"Erro verificando conteudo {url}: {e}")
        return 0.0


# ============================================================
# Busca de Site do Advogado
# ============================================================

def verificar_dominios_adv(slugs):
    """
    Testa dominios .adv.br e .com.br comuns para os slugs fornecidos.
    Retorna URL do site encontrado ou None.
    """
    extensoes = [".adv.br", ".com.br"]
    protocolos = ["https://", "https://www.", "http://"]

    for slug in slugs:
        for ext in extensoes:
            for proto in protocolos:
                url = f"{proto}{slug}{ext}"
                logger.debug(f"  Testando dominio: {url}")
                resultado = validar_url(url)
                if resultado:
                    logger.info(f"  ✅ Dominio ativo: {resultado}")
                    return resultado
    return None


def buscar_site_google(nome, nome_escritorio=None, cidade=None, estado=None):
    """
    Busca site do advogado via Google Search.
    Filtra dominios irrelevantes e prioriza .adv.br.
    """
    queries = []

    if nome_escritorio:
        queries.append(f'"{nome_escritorio}" advogado site')
        queries.append(f'"{nome_escritorio}" advocacia')
    queries.append(f'"{nome}" advogado {cidade or ""} {estado or ""} site')

    for query in queries:
        try:
            resp = requests.get(
                f"https://www.google.com/search?q={quote_plus(query)}&num=10",
                headers=_headers(),
                timeout=12,
            )
            if resp.status_code != 200:
                logger.debug(f"  Google retornou {resp.status_code} para: {query}")
                time.sleep(3)
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Coletar URLs candidatas
            candidatas_adv = []  # .adv.br tem prioridade
            candidatas_outras = []

            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]

                # Extrair URL real do Google
                url = None
                if "/url?q=" in href:
                    url = href.split("/url?q=")[1].split("&")[0]
                elif href.startswith("http") and "google" not in href:
                    url = href

                if not url:
                    continue

                # Filtrar dominios excluidos
                url_lower = url.lower()
                if any(excl in url_lower for excl in DOMINIOS_EXCLUIR):
                    continue

                # Classificar
                if ".adv.br" in url_lower:
                    candidatas_adv.append(url)
                elif any(url_lower.endswith(ext) or f"{ext}/" in url_lower
                         for ext in [".com.br", ".com", ".br", ".net.br"]):
                    candidatas_outras.append(url)

            # Testar candidatas .adv.br primeiro
            for url in candidatas_adv[:5]:
                resultado = validar_url(url)
                if resultado:
                    logger.info(f"  ✅ Site .adv.br encontrado via Google: {resultado}")
                    return resultado

            # Depois outras
            for url in candidatas_outras[:5]:
                resultado = validar_url(url)
                if resultado:
                    logger.info(f"  ✅ Site encontrado via Google: {resultado}")
                    return resultado

            time.sleep(2)

        except Exception as e:
            logger.debug(f"  Erro busca Google: {e}")

    return None


def buscar_site_advogado(adv):
    """
    Pipeline completo de busca de site para o advogado.
    Tenta multiplas estrategias em ordem de confiabilidade.

    Retorna dict com url, fonte, confianca ou None.
    """
    nome = adv.get("nome", "") if isinstance(adv, dict) else adv["nome"]
    nome_escritorio = (adv.get("nome_escritorio") if isinstance(adv, dict) else adv["nome_escritorio"]) or nome
    cidade = adv.get("cidade", "") if isinstance(adv, dict) else (adv["cidade"] or "")
    estado = adv.get("estado", "") if isinstance(adv, dict) else (adv["estado"] or "")

    logger.info(f"🔍 Buscando site: {nome} ({nome_escritorio}) — {cidade}/{estado}")

    # Etapa 1: Verificar dominios .adv.br / .com.br comuns
    slugs = _gerar_slugs_escritorio(nome_escritorio, nome)
    logger.info(f"   Slugs gerados: {slugs}")

    site_url = verificar_dominios_adv(slugs)
    if site_url:
        confianca = verificar_conteudo_site(site_url, nome, nome_escritorio)
        confianca = max(confianca, 0.6)  # Dominio direto = minimo 0.6
        logger.info(f"   ✅ SITE via dominio: {site_url} (confianca: {confianca:.0%})")
        return {"url": site_url, "fonte": "dominio_direto", "confianca": confianca}

    # Etapa 2: Busca Google
    site_url = buscar_site_google(nome, nome_escritorio, cidade, estado)
    if site_url:
        confianca = verificar_conteudo_site(site_url, nome, nome_escritorio)
        confianca = max(confianca, 0.4)  # Google = minimo 0.4
        logger.info(f"   ✅ SITE via Google: {site_url} (confianca: {confianca:.0%})")
        return {"url": site_url, "fonte": "google_search", "confianca": confianca}

    logger.info(f"   ❌ Nenhum site encontrado para {nome}")
    return None


# ============================================================
# Busca de Redes Sociais
# ============================================================

def buscar_instagram(nome, nome_escritorio=None, cidade=None):
    """Busca perfil do Instagram via Google."""
    queries = []
    if nome_escritorio:
        queries.append(f'site:instagram.com "{nome_escritorio}" advogado')
    queries.append(f'site:instagram.com "{nome}" advogado {cidade or ""}')

    for query in queries:
        try:
            resp = requests.get(
                f"https://www.google.com/search?q={quote_plus(query)}&num=5",
                headers=_headers(), timeout=10,
            )
            if resp.status_code != 200:
                continue

            matches = re.findall(r"instagram\.com/([a-zA-Z0-9_.]+)", resp.text)
            excluir = {"explore", "accounts", "p", "stories", "reel", "reels", "about", "directory"}

            for username in matches:
                if username.lower() not in excluir and len(username) >= 3:
                    return f"@{username}"

            time.sleep(1)
        except Exception:
            pass
    return None


def buscar_linkedin(nome, cidade=None):
    """Busca perfil do LinkedIn via Google."""
    query = f'site:linkedin.com/in "{nome}" advogado {cidade or ""}'
    try:
        resp = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=5",
            headers=_headers(), timeout=10,
        )
        if resp.status_code == 200:
            match = re.search(r"linkedin\.com/in/([a-zA-Z0-9_-]+)", resp.text)
            if match:
                return match.group(1)
    except Exception:
        pass
    return None


def buscar_facebook(nome, nome_escritorio=None, cidade=None):
    """Busca pagina do Facebook via Google."""
    queries = []
    if nome_escritorio:
        queries.append(f'site:facebook.com "{nome_escritorio}" advogado')
    queries.append(f'site:facebook.com "{nome}" advogado {cidade or ""}')

    for query in queries:
        try:
            resp = requests.get(
                f"https://www.google.com/search?q={quote_plus(query)}&num=5",
                headers=_headers(), timeout=10,
            )
            if resp.status_code != 200:
                continue

            matches = re.findall(r"facebook\.com/([a-zA-Z0-9_.]+)", resp.text)
            excluir = {"login", "pages", "groups", "events", "watch", "marketplace", "help"}

            for page in matches:
                if page.lower() not in excluir and len(page) >= 3:
                    return page

            time.sleep(1)
        except Exception:
            pass
    return None


def buscar_redes_completas(adv):
    """Busca todas as redes sociais do advogado."""
    nome = adv.get("nome", "") if isinstance(adv, dict) else adv["nome"]
    nome_escritorio = (adv.get("nome_escritorio") if isinstance(adv, dict) else adv["nome_escritorio"]) or nome
    cidade = adv.get("cidade", "") if isinstance(adv, dict) else (adv["cidade"] or "")

    resultado = {"instagram": None, "facebook": None, "linkedin": None}

    # Instagram
    ig_atual = adv.get("instagram") if isinstance(adv, dict) else adv["instagram"]
    if not ig_atual:
        resultado["instagram"] = buscar_instagram(nome, nome_escritorio, cidade)
        time.sleep(1)

    # LinkedIn
    li_atual = adv.get("linkedin") if isinstance(adv, dict) else adv["linkedin"]
    if not li_atual:
        resultado["linkedin"] = buscar_linkedin(nome, cidade)
        time.sleep(1)

    # Facebook
    fb_atual = adv.get("facebook") if isinstance(adv, dict) else adv["facebook"]
    if not fb_atual:
        resultado["facebook"] = buscar_facebook(nome, nome_escritorio, cidade)

    return resultado


# ============================================================
# Verificacao OAB CNA
# ============================================================

def verificar_oab_cna(numero_oab, seccional):
    """
    Verifica registro no Cadastro Nacional de Advogados da OAB.
    Retorna dict com dados da OAB ou None se nao encontrado.
    """
    if not numero_oab or not seccional:
        return None

    url = "https://cna.oab.org.br/search"

    try:
        payload = {
            "IsMobile": False,
            "NomeAdvo": "",
            "Inscricao": numero_oab.replace(".", "").strip(),
            "Uf": seccional.upper(),
            "TipoInsc": "",
            "PageIndex": 1,
            "PageSize": 5,
        }

        resp = requests.post(
            url, json=payload,
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Referer": "https://cna.oab.org.br/",
            },
            timeout=15,
        )

        if resp.status_code != 200:
            return None

        data = resp.json()
        for item in data.get("Data", []):
            return {
                "nome": item.get("Nome", "").strip(),
                "numero_oab": item.get("Inscricao", "").strip(),
                "seccional": item.get("UF", seccional),
                "situacao": item.get("TipoSituacao", ""),
                "tipo_inscricao": item.get("TipoInscricao", ""),
                "valido": True,
            }

        return {"valido": False, "motivo": "Nao encontrado no CNA"}

    except Exception as e:
        logger.debug(f"Erro OAB CNA: {e}")
        return None


# ============================================================
# Busca de Email
# ============================================================

def buscar_email_google(nome, cidade, estado):
    """Tenta encontrar email do advogado via Google."""
    query = f'"{nome}" advogado {cidade or ""} {estado or ""} email "@"'

    try:
        resp = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=5",
            headers=_headers(), timeout=10,
        )
        if resp.status_code != 200:
            return None

        # Extrair emails
        emails = re.findall(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            resp.text,
        )

        # Filtrar emails genericos/invalidos
        excluir = [
            "google", "gstatic", "example", "sentry", "schema.org",
            "w3.org", "googleapis", "microsoft", "apple", "yahoo",
            "noreply", "no-reply", "support@", "info@google",
        ]
        validos = [
            e for e in emails
            if not any(x in e.lower() for x in excluir)
        ]

        if validos:
            return validos[0]

    except Exception:
        pass
    return None


# ============================================================
# Inferencia de Dados
# ============================================================

MAPEAMENTO_AREAS = {
    "trabalhist": "Direito Trabalhista",
    "trabalho": "Direito Trabalhista",
    "criminal": "Direito Criminal",
    "penal": "Direito Criminal",
    "civil": "Direito Civil",
    "consumidor": "Direito do Consumidor",
    "consumerista": "Direito do Consumidor",
    "empresarial": "Direito Empresarial",
    "societari": "Direito Empresarial",
    "familia": "Direito de Familia",
    "divorcio": "Direito de Familia",
    "previdenciari": "Direito Previdenciario",
    "inss": "Direito Previdenciario",
    "tributari": "Direito Tributario",
    "fiscal": "Direito Tributario",
    "imobiliari": "Direito Imobiliario",
    "ambiental": "Direito Ambiental",
    "digital": "Direito Digital",
    "tecnologia": "Direito Digital",
    "medico": "Direito Medico",
    "saude": "Direito Medico",
    "bancari": "Direito Bancario",
    "condominim": "Direito Condominial",
    "agrari": "Direito Agrario",
    "eleitoral": "Direito Eleitoral",
    "administrativ": "Direito Administrativo",
    "contratual": "Contratos",
    "contrato": "Contratos",
}


def inferir_areas_por_nome(nome_escritorio):
    """Infere areas de atuacao com base no nome do escritorio."""
    if not nome_escritorio:
        return []

    nome = _normalizar(nome_escritorio)
    areas = []

    for keyword, area in MAPEAMENTO_AREAS.items():
        if keyword in nome and area not in areas:
            areas.append(area)

    return areas


def inferir_areas_por_site(url):
    """Tenta inferir areas de atuacao a partir do conteudo do site."""
    areas = []
    try:
        resp = requests.get(url, timeout=12, headers=_headers(), verify=False)
        if resp.status_code >= 400:
            return areas

        text = _normalizar(resp.text)

        for keyword, area in MAPEAMENTO_AREAS.items():
            if keyword in text and area not in areas:
                areas.append(area)

    except Exception:
        pass
    return areas


def inferir_porte(nome_escritorio):
    """Infere porte do escritorio pelo nome."""
    if not nome_escritorio:
        return "Solo"

    nome = _normalizar(nome_escritorio)

    if any(x in nome for x in ["associados", "partners", "grupo"]):
        return "Pequeno"
    if "&" in nome_escritorio or " e " in nome.split("advog")[0]:
        return "Pequeno"
    if any(x in nome for x in ["holding", "grupo", "rede"]):
        return "Medio"

    return "Solo"


# ============================================================
# Score de Potencial
# ============================================================

def calcular_score(adv):
    """Calcula score de potencial (0-100)."""
    score = 0
    d = dict(adv) if not isinstance(adv, dict) else adv

    # SEM SITE = principal criterio (+30)
    if not d.get("tem_site"):
        score += 30

    # Presenca digital parcial (+20)
    redes = sum(1 for k in ("instagram", "facebook", "linkedin") if d.get(k))
    if redes >= 1 and not d.get("tem_site"):
        score += min(redes * 7, 20)

    # Google Maps com reviews (+15)
    if d.get("google_avaliacao") and d["google_avaliacao"] >= 4.0:
        score += 10
    if d.get("google_reviews") and d["google_reviews"] >= 5:
        score += 5

    # Tempo de atuacao (+10)
    tempo = d.get("tempo_atuacao")
    if tempo:
        score += 10 if tempo >= 5 else (5 if tempo >= 2 else 0)

    # Volume de processos (+10)
    volume = d.get("volume_processos")
    if volume:
        score += 10 if volume >= 10 else (5 if volume >= 5 else 0)

    # Porte do escritorio (+10)
    porte = d.get("porte_escritorio", "")
    if porte in ("Pequeno", "Medio", "Médio"):
        score += 10
    elif porte == "Solo":
        score += 5

    # Multiplas areas (+5)
    try:
        areas = json.loads(d.get("areas_atuacao") or "[]")
    except (json.JSONDecodeError, TypeError):
        areas = []
    if len(areas) >= 3:
        score += 5

    return min(score, 100)


# ============================================================
# Pipeline Principal de Enriquecimento
# ============================================================

def enriquecer_advogado(adv_id, buscar_site_flag=True, buscar_redes_flag=True,
                        buscar_email_flag=True, verificar_oab_flag=True):
    """
    Pipeline completo de enriquecimento para um advogado.

    Etapas:
    1. Buscar site (dominios comuns + Google Search)
    2. Buscar/atualizar redes sociais
    3. Verificar OAB CNA
    4. Inferir areas e porte
    5. Buscar email (se nao tiver)
    6. Recalcular score

    Retorna dict com resultado detalhado.
    """
    db = _get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()

    if not adv:
        db.close()
        logger.warning(f"Advogado {adv_id} nao encontrado")
        return None

    adv_dict = dict(adv)
    atualizacoes = {}
    etapas = []

    logger.info(f"\n{'='*60}")
    logger.info(f"ENRIQUECENDO: {adv_dict['nome']} (ID: {adv_id})")
    logger.info(f"{'='*60}")

    # ── Etapa 1: Buscar Site ──────────────────────────────
    if buscar_site_flag:
        logger.info("📌 Etapa 1: Busca de Site")

        resultado_site = buscar_site_advogado(adv_dict)
        if resultado_site:
            atualizacoes["tem_site"] = 1
            atualizacoes["site_url"] = resultado_site["url"]
            etapas.append({
                "etapa": "busca_site",
                "resultado": "encontrado",
                "url": resultado_site["url"],
                "fonte": resultado_site["fonte"],
                "confianca": resultado_site["confianca"],
            })
            logger.info(f"   ✅ Site: {resultado_site['url']} ({resultado_site['fonte']}, "
                         f"confianca: {resultado_site['confianca']:.0%})")
        else:
            etapas.append({"etapa": "busca_site", "resultado": "nao_encontrado"})
            logger.info("   ❌ Nenhum site encontrado")

        time.sleep(1)

    # ── Etapa 2: Redes Sociais ────────────────────────────
    if buscar_redes_flag:
        logger.info("📌 Etapa 2: Redes Sociais")

        redes = buscar_redes_completas(adv_dict)
        redes_encontradas = {}

        if redes["instagram"] and not adv_dict.get("instagram"):
            atualizacoes["instagram"] = redes["instagram"]
            redes_encontradas["instagram"] = redes["instagram"]

        if redes["linkedin"] and not adv_dict.get("linkedin"):
            atualizacoes["linkedin"] = redes["linkedin"]
            redes_encontradas["linkedin"] = redes["linkedin"]

        if redes["facebook"] and not adv_dict.get("facebook"):
            atualizacoes["facebook"] = redes["facebook"]
            redes_encontradas["facebook"] = redes["facebook"]

        etapas.append({
            "etapa": "redes_sociais",
            "encontradas": redes_encontradas,
        })

        for rede, valor in redes_encontradas.items():
            logger.info(f"   ✅ {rede}: {valor}")

        if not redes_encontradas:
            logger.info("   — Nenhuma nova rede social encontrada")

        time.sleep(1)

    # ── Etapa 3: Verificar OAB ────────────────────────────
    if verificar_oab_flag and adv_dict.get("numero_oab"):
        logger.info("📌 Etapa 3: Verificacao OAB")

        oab_result = verificar_oab_cna(adv_dict["numero_oab"], adv_dict.get("seccional_oab", ""))

        if oab_result:
            if oab_result.get("valido"):
                atualizacoes["situacao_oab"] = oab_result.get("situacao", "")
                if oab_result.get("tipo_inscricao"):
                    atualizacoes["tipo_inscricao"] = oab_result["tipo_inscricao"]
                etapas.append({"etapa": "oab", "resultado": "valido", "dados": oab_result})
                logger.info(f"   ✅ OAB valida: {oab_result.get('situacao')}")
            else:
                etapas.append({"etapa": "oab", "resultado": "invalido", "motivo": oab_result.get("motivo")})
                logger.info(f"   ⚠️ OAB invalida: {oab_result.get('motivo')}")
        else:
            etapas.append({"etapa": "oab", "resultado": "erro_consulta"})
            logger.info("   — Nao foi possivel verificar OAB")

        time.sleep(1)

    # ── Etapa 4: Inferir Dados ────────────────────────────
    logger.info("📌 Etapa 4: Inferencia de Dados")

    # Areas de atuacao
    areas_atuais = []
    try:
        areas_atuais = json.loads(adv_dict.get("areas_atuacao") or "[]")
    except (json.JSONDecodeError, TypeError):
        pass

    if not areas_atuais or areas_atuais == []:
        areas_nome = inferir_areas_por_nome(adv_dict.get("nome_escritorio") or adv_dict["nome"])

        # Tambem tentar inferir do site, se encontrou
        site_url = atualizacoes.get("site_url") or adv_dict.get("site_url")
        if site_url:
            areas_site = inferir_areas_por_site(site_url)
            for area in areas_site:
                if area not in areas_nome:
                    areas_nome.append(area)

        if areas_nome:
            atualizacoes["areas_atuacao"] = json.dumps(areas_nome, ensure_ascii=False)
            logger.info(f"   ✅ Areas inferidas: {areas_nome}")
        else:
            logger.info("   — Nao foi possivel inferir areas")

    # Porte
    if not adv_dict.get("porte_escritorio"):
        porte = inferir_porte(adv_dict.get("nome_escritorio"))
        atualizacoes["porte_escritorio"] = porte
        logger.info(f"   ✅ Porte inferido: {porte}")

    # ── Etapa 5: Buscar Email ─────────────────────────────
    if buscar_email_flag and not adv_dict.get("email") and adv_dict.get("cidade"):
        logger.info("📌 Etapa 5: Busca de Email")

        email = buscar_email_google(
            adv_dict["nome"],
            adv_dict["cidade"],
            adv_dict.get("estado", ""),
        )
        if email:
            atualizacoes["email"] = email
            etapas.append({"etapa": "email", "resultado": "encontrado", "email": email})
            logger.info(f"   ✅ Email: {email}")
        else:
            etapas.append({"etapa": "email", "resultado": "nao_encontrado"})
            logger.info("   — Email nao encontrado")

        time.sleep(1)

    # ── Etapa 6: Aplicar atualizacoes e recalcular score ──
    logger.info("📌 Etapa 6: Salvando e recalculando score")

    if atualizacoes:
        sets = ", ".join(f"{k} = ?" for k in atualizacoes.keys())
        values = list(atualizacoes.values()) + [adv_id]
        db.execute(f"UPDATE advogados SET {sets} WHERE id = ?", values)
        logger.info(f"   Campos atualizados: {list(atualizacoes.keys())}")

    # Recalcular score com dados atualizados
    adv_atualizado = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()
    score = calcular_score(dict(adv_atualizado))
    db.execute("UPDATE advogados SET score_potencial = ? WHERE id = ?", (score, adv_id))

    db.commit()
    db.close()

    logger.info(f"   ✅ Score final: {score}")
    logger.info(f"{'='*60}\n")

    return {
        "adv_id": adv_id,
        "nome": adv_dict["nome"],
        "atualizacoes": atualizacoes,
        "etapas": etapas,
        "score_anterior": adv_dict.get("score_potencial", 0),
        "score_novo": score,
    }


# ============================================================
# Enriquecimento em Lote
# ============================================================

def enriquecer_todos(limite=100, buscar_site_flag=True, buscar_redes_flag=True,
                     buscar_email_flag=True, verificar_oab_flag=True):
    """
    Enriquece todos os advogados que precisam de atualizacao.
    Prioriza os com score mais baixo ou dados incompletos.
    """
    db = _get_db()

    advogados = db.execute("""
        SELECT id, nome FROM advogados
        WHERE tem_site = 0
           OR score_potencial = 0
           OR areas_atuacao IS NULL
           OR areas_atuacao = '[]'
           OR porte_escritorio IS NULL
        ORDER BY score_potencial ASC, data_criacao DESC
        LIMIT ?
    """, (limite,)).fetchall()

    db.close()

    total = len(advogados)
    resultados = []
    sucesso = 0
    erros = 0

    logger.info(f"\n🚀 Enriquecendo {total} advogados...\n")

    for i, adv in enumerate(advogados, 1):
        try:
            logger.info(f"[{i}/{total}] Processando: {adv['nome']} (ID: {adv['id']})")

            resultado = enriquecer_advogado(
                adv["id"],
                buscar_site_flag=buscar_site_flag,
                buscar_redes_flag=buscar_redes_flag,
                buscar_email_flag=buscar_email_flag,
                verificar_oab_flag=verificar_oab_flag,
            )

            if resultado:
                resultados.append(resultado)
                sucesso += 1
            else:
                erros += 1

        except Exception as e:
            logger.error(f"Erro ao enriquecer {adv['nome']}: {e}")
            erros += 1

        # Rate limiting entre advogados
        time.sleep(2)

    logger.info(f"\n✅ Enriquecimento concluido: {sucesso} sucesso, {erros} erros\n")

    return {
        "total_processados": total,
        "sucesso": sucesso,
        "erros": erros,
        "resultados": resultados,
    }


def recalcular_todos_scores():
    """Recalcula o score de todos os advogados."""
    db = _get_db()
    advogados = db.execute("SELECT * FROM advogados").fetchall()

    for adv in advogados:
        score = calcular_score(dict(adv))
        db.execute("UPDATE advogados SET score_potencial = ? WHERE id = ?", (score, adv["id"]))

    db.commit()
    count = len(advogados)
    db.close()

    logger.info(f"Scores recalculados para {count} advogados")
    return count


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    import sys

    print("\n" + "=" * 60)
    print("ProspectAdv — Enriquecimento de Dados v2")
    print("=" * 60)

    if len(sys.argv) > 1 and sys.argv[1] == "--id":
        # Enriquecer advogado especifico
        adv_id = int(sys.argv[2])
        print(f"\nEnriquecendo advogado ID: {adv_id}")
        resultado = enriquecer_advogado(adv_id)
        if resultado:
            print(f"\n✅ Concluido!")
            print(f"   Score: {resultado['score_anterior']} → {resultado['score_novo']}")
            print(f"   Campos atualizados: {list(resultado['atualizacoes'].keys())}")
        else:
            print("❌ Advogado nao encontrado")
    else:
        # Enriquecer todos
        print("\nEnriquecendo todos os advogados...")
        resultado = enriquecer_todos(limite=50)
        print(f"\n✅ Concluido: {resultado['sucesso']} sucesso, {resultado['erros']} erros")
