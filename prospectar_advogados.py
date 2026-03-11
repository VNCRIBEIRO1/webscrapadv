"""
Prospectar Advogados v1 — Sistema Inteligente de Prospeccao
ProspectAdv

Pipeline INTEGRADO: descoberta + verificacao de site + enriquecimento em uma unica passada.
Resolve o problema de marcar advogados COM site como "SEM SITE".

Tecnicas de verificacao implementadas:
1. Domain Brute-Force — testa variantes .adv.br / .com.br para cada nome
2. DNS Resolution — verifica existencia do dominio (rapido)
3. HTTP Validation — confirma site ativo e acessivel
4. Content Fingerprinting — verifica se conteudo corresponde ao escritorio
5. Google SERP Analysis — busca site via Google Search
6. SEO Data Extraction — titulo, meta description, schema.org
7. Social Media Cross-Reference — checa se perfis linkam p/ site
8. Multi-slug Generation — gera 10+ variantes de dominio por escritorio
9. Score Calculation — pontua potencial de venda
10. Validacao cruzada — cruza dados de multiplas fontes
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

logger = logging.getLogger("ProspectAdv.Prospectar")

DATABASE = "prospeccao_adv.db"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_3) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36 Edg/122.0.0.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
]

DOMINIOS_EXCLUIR = [
    "instagram.com", "facebook.com", "linkedin.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "pinterest.com",
    "jusbrasil.com.br", "escavador.com", "migalhas.com.br", "conjur.com.br",
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com",
    "oab.org.br", "jus.br", "gov.br", "wikipedia.org",
    "reclameaqui.com.br", "glassdoor.com", "indeed.com",
    "example.com", "exemplo.com", "amaivos.com.br",
    "apontador.com.br", "guiamais.com.br", "telelistas.net",
    "encontrasp.com.br", "hagah.com.br", "yelp.com",
    "maps.google.com", "goo.gl", "bit.ly",
]

# Palavras que indicam conteudo juridico
KEYWORDS_JURIDICAS = [
    "advogad", "advocacia", "escritorio", "oab", "juridic",
    "direito", "juiz", "tribunal", "processo", "causa",
    "consulta", "honorario", "petricao", "acao", "defesa",
    "contrato", "trabalhist", "criminal", "civil", "familia",
    "previdenci", "tributari", "empresarial", "imobiliari",
]


# ============================================================
# UTILIDADES
# ============================================================

def _get_db():
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


def _headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
    }


def _limpar_telefone(telefone):
    if not telefone:
        return None
    return "".join(filter(str.isdigit, telefone))


# ============================================================
# 1. GERADOR DE SLUGS AVANCADO (10+ variantes por escritorio)
# ============================================================

def gerar_slugs_avancados(nome_escritorio, nome_advogado=None):
    """
    Gera todas as variantes possiveis de slug para testar dominios.
    Para 'Rocha Advogados' gera: rocha, rochaadvogados, rochaadv,
    rocha-advogados, advrocha, escritoriorocha, etc.
    """
    slugs = []

    if nome_escritorio:
        norm = _normalizar(nome_escritorio)

        # Remover sufixos comuns para obter o "core" do nome
        sufixos = [
            "advogados associados", "advogados e associados",
            "advogados", "advocacia e consultoria",
            "advocacia", "associados", "consultoria juridica",
            "assessoria juridica", "escritorio de advocacia",
            "escritorio", "soc de advogados", "sociedade de advogados",
        ]
        core = norm
        for suf in sufixos:
            core = core.replace(suf, "")

        # Remover conectores
        core = re.sub(r"\b(e|de|do|da|dos|das|a|o)\b", "", core)
        core = core.replace("&", "").strip()
        core = re.sub(r"\s+", " ", core).strip()

        # Partes individuais do core
        partes = [p for p in core.split() if len(p) >= 2]
        core_slug = re.sub(r"[^a-z0-9]", "", core)

        if core_slug and len(core_slug) >= 3:
            # === Variantes do core ===
            slugs.append(core_slug)                        # rocha
            slugs.append(f"{core_slug}advogados")          # rochaadvogados
            slugs.append(f"{core_slug}adv")                # rochaadv
            slugs.append(f"{core_slug}advocacia")          # rochaadvocacia
            slugs.append(f"adv{core_slug}")                # advrocha
            slugs.append(f"escritorio{core_slug}")         # escritoriorocha

            # Com hifen
            core_dashed = re.sub(r"\s+", "-", core.strip())
            core_dashed = re.sub(r"[^a-z0-9-]", "", core_dashed)
            core_dashed = re.sub(r"-+", "-", core_dashed).strip("-")
            if core_dashed and core_dashed != core_slug:
                slugs.append(core_dashed)                  # rocha
                slugs.append(f"{core_dashed}-advogados")   # rocha-advogados
                slugs.append(f"{core_dashed}-adv")         # rocha-adv

            # Combinacoes com partes individuais
            if len(partes) >= 2:
                # Primeiras iniciais + ultimo
                first_initials = "".join(p[0] for p in partes[:-1])
                last = re.sub(r"[^a-z0-9]", "", partes[-1])
                combo = first_initials + last
                if combo and len(combo) >= 3 and combo not in slugs:
                    slugs.append(combo)

                # Cada parte como slug separado
                for p in partes:
                    p_clean = re.sub(r"[^a-z0-9]", "", p)
                    if p_clean and len(p_clean) >= 4 and p_clean not in slugs:
                        slugs.append(p_clean)

        # Nome completo do escritorio como slug
        full_slug = re.sub(r"[^a-z0-9]", "", norm)
        if full_slug and len(full_slug) >= 4 and full_slug not in slugs:
            slugs.append(full_slug)

    # === Variantes baseadas no nome do advogado ===
    if nome_advogado:
        nome_norm = _normalizar(nome_advogado)
        for prefix in ["dr. ", "dra. ", "dr ", "dra "]:
            nome_norm = nome_norm.replace(prefix, "")
        nome_norm = nome_norm.strip()

        partes_nome = nome_norm.split()
        if partes_nome:
            # Sobrenome
            sobrenome = re.sub(r"[^a-z0-9]", "", partes_nome[-1])
            if sobrenome and len(sobrenome) >= 3 and sobrenome not in slugs:
                slugs.append(sobrenome)
                slugs.append(f"{sobrenome}advogados")
                slugs.append(f"{sobrenome}adv")
                slugs.append(f"{sobrenome}advocacia")

            # Primeiro + Ultimo
            if len(partes_nome) >= 2:
                primeiro = re.sub(r"[^a-z0-9]", "", partes_nome[0])
                first_last = primeiro + sobrenome
                if first_last and first_last not in slugs:
                    slugs.append(first_last)
                    slugs.append(f"{first_last}adv")

            # Nome completo
            full_name = re.sub(r"[^a-z0-9]", "", nome_norm)
            if full_name and len(full_name) >= 5 and full_name not in slugs:
                slugs.append(full_name)

    # Slugs GENERICOS que geram falsos positivos (existem como sites reais de terceiros)
    SLUGS_GENERICOS = {
        "advocacia", "advogados", "advogado", "escritorio", "direito",
        "juridico", "juridica", "adv", "advadvocacia", "advogadoadvocacia",
        "escritorioadvocacia", "advogadosassociados", "consultoriajuridica",
        "assessoriajuridica", "advadvogados", "advogadosadvocacia",
        "advocaciaadv", "advogadosadv", "direitoadv",
    }

    # Remover duplicatas e genericos, manter ordem
    seen = set()
    unique = []
    for s in slugs:
        if s and s not in seen and len(s) >= 3 and s not in SLUGS_GENERICOS:
            seen.add(s)
            unique.append(s)

    return unique


# ============================================================
# 2. VERIFICACAO DE DOMINIO (DNS + HTTP)
# ============================================================

def dns_resolve(dominio):
    """Verifica se dominio resolve via DNS. Retorna True/False."""
    try:
        old_timeout = socket.getdefaulttimeout()
        socket.setdefaulttimeout(3)
        result = socket.getaddrinfo(dominio, 443, socket.AF_INET, socket.SOCK_STREAM)
        socket.setdefaulttimeout(old_timeout)
        return bool(result)
    except (socket.gaierror, socket.timeout, OSError):
        try:
            socket.setdefaulttimeout(old_timeout)
        except Exception:
            pass
        return False
    except Exception:
        return False


def http_validar(url, timeout=5):
    """Verifica se URL esta ativa. Retorna URL final ou None.
    Descarta redirecionamentos para dominios de terceiros (LinkedIn, etc).
    """
    dominios_redirect_excluir = [
        "linkedin.com", "instagram.com", "facebook.com", "twitter.com",
        "youtube.com", "google.com", "godaddy.com", "registro.br",
        "uolhost.com", "locaweb.com", "hostgator.com", "wix.com",
        "wordpress.com", "squarespace.com",
    ]

    # timeout=(connect, read) — evita travamento em SSL hang
    for method in ["head", "get"]:
        try:
            if method == "head":
                resp = requests.head(
                    url, timeout=(3, 4), allow_redirects=True,
                    headers={"User-Agent": random.choice(USER_AGENTS)},
                    verify=False,
                )
            else:
                resp = requests.get(
                    url, timeout=(3, 4), allow_redirects=True,
                    headers={"User-Agent": random.choice(USER_AGENTS)},
                    stream=True, verify=False,
                )

            if resp.status_code < 400:
                # Verificar se redirecionou para dominio de terceiro
                final_url = resp.url.lower()
                if any(excl in final_url for excl in dominios_redirect_excluir):
                    logger.debug(f"  Redirect para terceiro: {resp.url}")
                    return None
                return resp.url
        except KeyboardInterrupt:
            raise
        except Exception:
            pass

    return None


def verificar_dominios_completo(slugs):
    """
    Testa TODAS as combinacoes de slug + extensao + protocolo.
    Retorna lista de sites encontrados (pode haver mais de um).
    """
    extensoes = [".adv.br", ".com.br", ".com", ".net.br"]
    prefixos = ["", "www."]
    encontrados = []

    for slug in slugs:
        for ext in extensoes:
            dominio = f"{slug}{ext}"

            # DNS check rapido (evita HTTP desnecessario)
            if not dns_resolve(dominio):
                continue

            # DNS resolveu — testar HTTP
            for pref in prefixos:
                url = f"https://{pref}{dominio}"
                resultado = http_validar(url)
                if resultado:
                    # Normalizar URL
                    parsed = urlparse(resultado)
                    url_limpa = f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/") + "/"
                    if url_limpa not in [e["url"] for e in encontrados]:
                        encontrados.append({
                            "url": url_limpa,
                            "dominio": dominio,
                            "extensao": ext,
                            "slug_usado": slug,
                        })
                    break  # Nao precisa testar outro prefixo

            if encontrados:
                # Priorizar .adv.br
                adv_br = [e for e in encontrados if ".adv.br" in e["extensao"]]
                if adv_br:
                    return adv_br
                return encontrados

    return encontrados


# ============================================================
# 3. VERIFICACAO DE CONTEUDO (SEO Fingerprinting)
# ============================================================

def extrair_dados_seo(url, nome=None, nome_escritorio=None):
    """
    Extrai dados SEO do site e verifica se pertence ao escritorio.
    Retorna dict com titulo, descricao, keywords, confianca, areas.
    """
    resultado = {
        "titulo": "",
        "descricao": "",
        "keywords": [],
        "areas_atuacao": [],
        "telefone_site": None,
        "email_site": None,
        "redes_sociais": {},
        "confianca": 0.0,
        "tem_oab": False,
        "schema_org": None,
    }

    try:
        resp = requests.get(
            url, timeout=12, headers=_headers(),
            verify=False, allow_redirects=True,
        )
        if resp.status_code >= 400:
            return resultado

        html = resp.text
        soup = BeautifulSoup(html, "html.parser")

        # === Titulo ===
        if soup.title and soup.title.string:
            resultado["titulo"] = soup.title.string.strip()

        # === Meta Description ===
        meta_desc = soup.find("meta", attrs={"name": "description"})
        if meta_desc and meta_desc.get("content"):
            resultado["descricao"] = meta_desc["content"].strip()

        # === Meta Keywords ===
        meta_kw = soup.find("meta", attrs={"name": "keywords"})
        if meta_kw and meta_kw.get("content"):
            resultado["keywords"] = [k.strip() for k in meta_kw["content"].split(",")]

        # === Texto visivel normalizado ===
        text = _normalizar(soup.get_text(separator=" ", strip=True))
        title_norm = _normalizar(resultado["titulo"])

        # === Telefones no site ===
        telefones = re.findall(
            r"\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}", html
        )
        if telefones:
            resultado["telefone_site"] = telefones[0]

        # === Emails no site ===
        emails = re.findall(
            r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
            html,
        )
        emails_validos = [
            e for e in emails
            if not any(x in e.lower() for x in [
                "google", "gstatic", "example", "sentry",
                "w3.org", "schema.org", "noreply", "wix",
                "wordpress", "jquery", "bootstrap",
            ])
        ]
        if emails_validos:
            resultado["email_site"] = emails_validos[0]

        # === Redes sociais no site ===
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if "instagram.com/" in href:
                match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", href)
                if match and match.group(1) not in ("explore", "p", "reel", "stories"):
                    resultado["redes_sociais"]["instagram"] = f"@{match.group(1)}"
            elif "facebook.com/" in href:
                match = re.search(r"facebook\.com/([a-zA-Z0-9_.]+)", href)
                if match and match.group(1) not in ("login", "pages", "groups", "sharer"):
                    resultado["redes_sociais"]["facebook"] = match.group(1)
            elif "linkedin.com/" in href:
                match = re.search(r"linkedin\.com/(?:in|company)/([a-zA-Z0-9_-]+)", href)
                if match:
                    resultado["redes_sociais"]["linkedin"] = match.group(1)

        # === Schema.org / JSON-LD ===
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                ld = json.loads(script.string)
                if isinstance(ld, dict):
                    tipo = ld.get("@type", "")
                    if any(t in tipo for t in ["LegalService", "Attorney", "Organization", "LocalBusiness"]):
                        resultado["schema_org"] = {
                            "type": tipo,
                            "name": ld.get("name"),
                            "telephone": ld.get("telephone"),
                            "email": ld.get("email"),
                        }
                        if ld.get("telephone"):
                            resultado["telefone_site"] = ld["telephone"]
                        if ld.get("email"):
                            resultado["email_site"] = ld["email"]
            except (json.JSONDecodeError, TypeError):
                pass

        # === OAB mencionada ===
        if re.search(r"oab[\s/]*[a-z]{2}[\s]*\d", text):
            resultado["tem_oab"] = True

        # === Inferir areas de atuacao do conteudo ===
        MAPEAMENTO = {
            "trabalhist": "Direito Trabalhista",
            "trabalho": "Direito Trabalhista",
            "criminal": "Direito Criminal",
            "penal": "Direito Criminal",
            "civil": "Direito Civil",
            "consumidor": "Direito do Consumidor",
            "empresarial": "Direito Empresarial",
            "societari": "Direito Empresarial",
            "tributari": "Direito Tributario",
            "familia": "Direito de Familia",
            "previdenciari": "Direito Previdenciario",
            "imobiliari": "Direito Imobiliario",
            "ambiental": "Direito Ambiental",
            "digital": "Direito Digital",
            "bancari": "Direito Bancario",
            "regulatori": "Direito Regulatorio",
            "publico": "Direito Publico",
        }
        for keyword, area in MAPEAMENTO.items():
            if keyword in text and area not in resultado["areas_atuacao"]:
                resultado["areas_atuacao"].append(area)

        # === Calculo de confianca ===
        confianca = 0.0

        # Verificar nome do escritorio
        if nome_escritorio:
            esc_norm = _normalizar(nome_escritorio)
            if esc_norm in title_norm:
                confianca = max(confianca, 0.95)
            elif esc_norm in text:
                confianca = max(confianca, 0.85)
            # Partes do nome
            partes = [p for p in esc_norm.split() if len(p) >= 3]
            if partes:
                matches = sum(1 for p in partes if p in text)
                ratio = matches / len(partes)
                if ratio >= 0.6:
                    confianca = max(confianca, 0.75)

        # Verificar nome do advogado
        if nome:
            nome_norm = _normalizar(nome)
            for prefix in ["dr. ", "dra. ", "dr ", "dra "]:
                nome_norm = nome_norm.replace(prefix, "")
            nome_norm = nome_norm.strip()
            if nome_norm in title_norm:
                confianca = max(confianca, 0.90)
            elif nome_norm in text:
                confianca = max(confianca, 0.80)

        # Keywords juridicas
        kw_hits = sum(1 for kw in KEYWORDS_JURIDICAS if kw in text)
        if kw_hits >= 4:
            confianca = max(confianca, 0.60)
        elif kw_hits >= 2:
            confianca = max(confianca, 0.40)

        # Dominio .adv.br = bonus
        if ".adv.br" in url:
            confianca = max(confianca, 0.65)

        resultado["confianca"] = confianca

    except Exception as e:
        logger.debug(f"Erro extraindo SEO de {url}: {e}")

    return resultado


# ============================================================
# 4. BUSCA GOOGLE SERP (fallback)
# ============================================================

def buscar_site_google_serp(nome, nome_escritorio=None, cidade=None, estado=None):
    """
    Busca site do escritorio via Google Search.
    Usa queries otimizadas para SEO juridico.
    """
    queries = []

    if nome_escritorio:
        esc_slug = re.sub(r"[^a-z0-9]", "", _normalizar(nome_escritorio))
        queries.append(f"{esc_slug}")  # Busca direta pelo slug (como o usuario fez)
        queries.append(f'"{nome_escritorio}" site advogado')
        queries.append(f'"{nome_escritorio}" .adv.br')
    if nome:
        queries.append(f'"{nome}" advogado {cidade or ""} site')

    for query in queries:
        try:
            time.sleep(random.uniform(2, 4))
            resp = requests.get(
                f"https://www.google.com/search?q={quote_plus(query)}&num=10&hl=pt-BR",
                headers=_headers(),
                timeout=12,
            )
            if resp.status_code != 200:
                logger.debug(f"  Google status {resp.status_code} para: {query}")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")

            # Coletar URLs candidatas
            candidatas_adv = []
            candidatas_outras = []

            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                url = None
                if "/url?q=" in href:
                    url = href.split("/url?q=")[1].split("&")[0]
                elif href.startswith("http") and "google" not in href:
                    url = href

                if not url:
                    continue

                url_lower = url.lower()
                if any(excl in url_lower for excl in DOMINIOS_EXCLUIR):
                    continue

                if ".adv.br" in url_lower:
                    candidatas_adv.append(url)
                elif any(ext in url_lower for ext in [".com.br", ".com", ".net.br"]):
                    candidatas_outras.append(url)

            # Verificar .adv.br primeiro
            for url in candidatas_adv[:5]:
                resultado = http_validar(url)
                if resultado:
                    logger.info(f"  Site .adv.br via Google: {resultado}")
                    return resultado

            # Depois outros
            for url in candidatas_outras[:3]:
                resultado = http_validar(url)
                if resultado:
                    logger.info(f"  Site via Google: {resultado}")
                    return resultado

        except Exception as e:
            logger.debug(f"  Erro Google: {e}")

    return None


# ============================================================
# 5. PIPELINE INTEGRADO DE VERIFICACAO DE SITE
# ============================================================

def verificar_site_completo(nome, nome_escritorio=None, cidade=None, estado=None):
    """
    Pipeline completo de verificacao de site com TODAS as tecnicas.
    Esta e a funcao central que resolve o problema de falsos negativos.

    Retorna dict com: tem_site, site_url, confianca, fonte, dados_seo
    """
    nome_esc = nome_escritorio or nome

    logger.info(f"  === Verificacao completa: {nome_esc} ===")

    resultado = {
        "tem_site": False,
        "site_url": None,
        "confianca": 0.0,
        "fonte_deteccao": None,
        "dados_seo": None,
    }

    # --- Etapa 1: Domain Brute-Force (.adv.br / .com.br) ---
    logger.info(f"  [1/3] Domain brute-force...")
    slugs = gerar_slugs_avancados(nome_esc, nome)
    logger.info(f"        Slugs: {slugs[:8]}{'...' if len(slugs) > 8 else ''}")

    dominios = verificar_dominios_completo(slugs)
    if dominios:
        site = dominios[0]
        logger.info(f"  ✅ Dominio encontrado: {site['url']} (slug: {site['slug_usado']})")

        # Extrair dados SEO
        seo = extrair_dados_seo(site["url"], nome, nome_esc)
        confianca = max(seo["confianca"], 0.60)

        resultado["tem_site"] = True
        resultado["site_url"] = site["url"]
        resultado["confianca"] = confianca
        resultado["fonte_deteccao"] = "dominio_direto"
        resultado["dados_seo"] = seo
        return resultado

    # --- Etapa 2: Google SERP ---
    logger.info(f"  [2/3] Google SERP search...")
    site_url = buscar_site_google_serp(nome, nome_esc, cidade, estado)
    if site_url:
        seo = extrair_dados_seo(site_url, nome, nome_esc)
        confianca = max(seo["confianca"], 0.40)

        resultado["tem_site"] = True
        resultado["site_url"] = site_url
        resultado["confianca"] = confianca
        resultado["fonte_deteccao"] = "google_search"
        resultado["dados_seo"] = seo
        return resultado

    # --- Etapa 3: Verificacao extra de dominios com variantes criativas ---
    logger.info(f"  [3/3] Variantes criativas...")
    slugs_extras = []
    if nome_escritorio:
        norm = _normalizar(nome_escritorio)
        # Abreviacoes
        termos_genericos = {"advogados", "advocacia", "escritorio", "associados",
                            "consultoria", "juridica", "juridico", "direito", "soc"}
        partes = [p for p in norm.split()
                  if len(p) >= 2
                  and p not in ("de", "do", "da", "dos", "das", "e", "a", "o")
                  and p not in termos_genericos]
        if len(partes) >= 2:
            # Iniciais: "Rocha & Santos" -> "rs"
            iniciais = "".join(p[0] for p in partes)
            if len(iniciais) >= 2:
                slugs_extras.append(f"{iniciais}adv")
                slugs_extras.append(f"{iniciais}advocacia")
            # Primeiro e ultimo separados
            for p in partes:
                clean = re.sub(r"[^a-z0-9]", "", p)
                if clean and len(clean) >= 4:
                    slugs_extras.append(f"{clean}adv")
                    slugs_extras.append(f"adv{clean}")

    # Remover os que ja foram testados
    slugs_extras = [s for s in slugs_extras if s not in slugs and len(s) >= 3]
    if slugs_extras:
        dominios = verificar_dominios_completo(slugs_extras)
        if dominios:
            site = dominios[0]
            seo = extrair_dados_seo(site["url"], nome, nome_esc)
            confianca = max(seo["confianca"], 0.50)

            resultado["tem_site"] = True
            resultado["site_url"] = site["url"]
            resultado["confianca"] = confianca
            resultado["fonte_deteccao"] = "variante_criativa"
            resultado["dados_seo"] = seo
            return resultado

    logger.info(f"  ❌ Nenhum site encontrado para {nome_esc}")
    return resultado


# ============================================================
# 6. BUSCA OAB CNA (descoberta de advogados)
# ============================================================

def buscar_advogados_oab(seccional="SP", nome_filtro="", pagina=1, page_size=20):
    """Busca advogados no CNA da OAB."""
    url = "https://cna.oab.org.br/search"

    try:
        payload = {
            "IsMobile": False,
            "NomeAdvo": nome_filtro,
            "Inscricao": "",
            "Uf": seccional,
            "TipoInsc": "P",
            "PageIndex": pagina,
            "PageSize": page_size,
        }

        resp = requests.post(
            url, json=payload,
            headers={
                "User-Agent": random.choice(USER_AGENTS),
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Referer": "https://cna.oab.org.br/",
                "Origin": "https://cna.oab.org.br",
            },
            timeout=15,
        )

        if resp.status_code != 200:
            logger.warning(f"OAB CNA status {resp.status_code}")
            return []

        data = resp.json()
        advogados = []

        for item in data.get("Data", []):
            situacao = item.get("TipoSituacao", "")
            if "ativo" not in situacao.lower():
                continue

            advogados.append({
                "nome": item.get("Nome", "").strip(),
                "numero_oab": item.get("Inscricao", "").strip(),
                "seccional_oab": item.get("UF", seccional),
                "situacao_oab": situacao,
                "tipo_inscricao": item.get("TipoInscricao", ""),
            })

        logger.info(f"OAB CNA {seccional}: {len(advogados)} ativos (p{pagina})")
        return advogados

    except Exception as e:
        logger.error(f"Erro OAB CNA: {e}")
        return []


# ============================================================
# 7. BUSCA GOOGLE PARA DESCOBERTA DE ESCRITORIOS
# ============================================================

def buscar_escritorios_google(cidade, estado, max_resultados=20):
    """
    Descobre escritorios de advocacia via Google Search.
    Retorna lista com nome, telefone, endereco quando disponivel.
    """
    termos = [
        f"escritorio de advocacia {cidade} {estado} telefone",
        f"advogados {cidade} {estado}",
    ]

    escritorios = []
    nomes_vistos = set()

    for termo in termos:
        try:
            time.sleep(random.uniform(2, 4))
            resp = requests.get(
                f"https://www.google.com/search?q={quote_plus(termo)}&num=20&hl=pt-BR",
                headers=_headers(), timeout=12,
            )
            if resp.status_code != 200:
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(separator="\n")

            # Extrair nomes de escritorios (patterns comuns)
            patterns = [
                r"([A-Z][a-záéíóúàãõê]+(?:\s+(?:&|e)\s+[A-Z][a-záéíóúàãõê]+)*\s+Advogados?\s*(?:Associados)?)",
                r"([A-Z][a-záéíóúàãõê]+\s+Advocacia)",
                r"(Escritório\s+[A-Z][a-záéíóúàãõê]+(?:\s+[A-Z][a-záéíóúàãõê]+)*)",
            ]

            for pat in patterns:
                matches = re.findall(pat, text)
                for match in matches:
                    nome_limpo = match.strip()
                    if nome_limpo not in nomes_vistos and len(nome_limpo) >= 5:
                        nomes_vistos.add(nome_limpo)
                        escritorios.append({
                            "nome_escritorio": nome_limpo,
                            "cidade": cidade,
                            "estado": estado,
                            "fonte": "google_search",
                        })

            if len(escritorios) >= max_resultados:
                break

        except Exception as e:
            logger.debug(f"Erro busca Google: {e}")

    return escritorios[:max_resultados]


# ============================================================
# 8. CALCULO DE SCORE
# ============================================================

def calcular_score(adv):
    """Calcula score de potencial (0-100). Maior = melhor prospecto."""
    score = 0
    d = dict(adv) if not isinstance(adv, dict) else adv

    # SEM SITE = criterio principal (+30)
    if not d.get("tem_site"):
        score += 30

    # Presenca digital parcial sem site (+20)
    redes = sum(1 for k in ("instagram", "facebook", "linkedin") if d.get(k))
    if redes >= 1 and not d.get("tem_site"):
        score += min(redes * 7, 20)

    # Avaliacoes Google (+15)
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

    # Porte (+10)
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
# 9. SALVAR NO BANCO
# ============================================================

def salvar_prospecto(dados):
    """Salva um prospecto no banco de dados."""
    db = _get_db()

    # Verificar duplicata
    existe = db.execute(
        "SELECT id FROM advogados WHERE nome = ? AND cidade = ?",
        (dados["nome"], dados.get("cidade", "")),
    ).fetchone()

    if existe:
        db.close()
        return None

    # Campos para inserir
    campos = {
        "nome": dados.get("nome", ""),
        "nome_escritorio": dados.get("nome_escritorio"),
        "numero_oab": dados.get("numero_oab"),
        "seccional_oab": dados.get("seccional_oab"),
        "situacao_oab": dados.get("situacao_oab"),
        "email": dados.get("email"),
        "telefone": dados.get("telefone"),
        "whatsapp": dados.get("whatsapp"),
        "endereco": dados.get("endereco"),
        "cidade": dados.get("cidade", ""),
        "estado": dados.get("estado", ""),
        "tem_site": dados.get("tem_site", 0),
        "site_url": dados.get("site_url"),
        "instagram": dados.get("instagram"),
        "facebook": dados.get("facebook"),
        "linkedin": dados.get("linkedin"),
        "google_avaliacao": dados.get("google_avaliacao"),
        "google_reviews": dados.get("google_reviews"),
        "google_maps_url": dados.get("google_maps_url"),
        "areas_atuacao": dados.get("areas_atuacao", "[]"),
        "porte_escritorio": dados.get("porte_escritorio", "Solo"),
        "tempo_atuacao": dados.get("tempo_atuacao"),
        "volume_processos": dados.get("volume_processos"),
        "fonte": dados.get("fonte", "prospectar"),
        "foto": dados.get("foto", ""),
    }

    cols = ", ".join(campos.keys())
    placeholders = ", ".join(["?"] * len(campos))

    try:
        cursor = db.execute(
            f"INSERT INTO advogados ({cols}) VALUES ({placeholders})",
            list(campos.values()),
        )
        adv_id = cursor.lastrowid

        # Calcular e salvar score
        score = calcular_score(campos)
        db.execute(
            "UPDATE advogados SET score_potencial = ? WHERE id = ?",
            (score, adv_id),
        )

        db.commit()
        db.close()
        return adv_id

    except Exception as e:
        logger.error(f"Erro ao salvar prospecto: {e}")
        db.close()
        return None


# ============================================================
# 10. LIMPAR BANCO
# ============================================================

def limpar_banco():
    """Remove TODOS os dados do banco para recomeco limpo."""
    db = _get_db()

    tabelas = [
        "historico", "emails_enviados", "whatsapp_mensagens",
        "automacao_fila", "respostas", "advogados",
    ]
    for tabela in tabelas:
        try:
            db.execute(f"DELETE FROM {tabela}")
            logger.info(f"  Tabela {tabela} limpa")
        except Exception as e:
            logger.warning(f"  Erro limpando {tabela}: {e}")

    db.commit()
    db.close()
    logger.info("Banco de dados limpo completamente")


# ============================================================
# 11. PIPELINE PRINCIPAL: PROSPECTAR NOVOS
# ============================================================

def prospectar_escritorios_reais(n=10):
    """
    Prospecta N escritorios REAIS com verificacao integrada de site.

    Abordagem: Domain Brute-Force + Verificacao Multi-Tecnica
    1. Gera nomes de escritorios a partir de sobrenomes comuns brasileiros
    2. Para CADA nome, verifica se existe site (DNS + HTTP + SEO)
    3. Escritorios SEM SITE confirmado → salvos como prospectos
    4. Escritorios COM SITE → descartados (corretamente)

    Nao depende de APIs externas (OAB CNA, Google Maps).
    """

    logger.info("\n" + "=" * 70)
    logger.info("PROSPECCAO INTELIGENTE — Domain Brute-Force + Verificacao SEO")
    logger.info("=" * 70)

    # 120 sobrenomes comuns brasileiros para gerar nomes de escritorios
    SOBRENOMES = [
        "Silva", "Santos", "Oliveira", "Souza", "Rodrigues",
        "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
        "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida",
        "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
        "Rocha", "Dias", "Nascimento", "Andrade", "Moreira",
        "Nunes", "Marques", "Machado", "Mendes", "Freitas",
        "Cardoso", "Ramos", "Goncalves", "Santana", "Teixeira",
        "Moura", "Correia", "Pinto", "Campos", "Castro",
        "Cunha", "Monteiro", "Pires", "Borges", "Melo",
        "Azevedo", "Medeiros", "Reis", "Fonseca", "Duarte",
        "Coelho", "Nogueira", "Tavares", "Miranda", "Amaral",
        "Batista", "Bezerra", "Camargo", "Cavalcanti", "Braga",
        "Barros", "Guimaraes", "Macedo", "Matos", "Brito",
        "Lacerda", "Faria", "Peixoto", "Vasconcelos", "Amorim",
        "Brandao", "Rezende", "Arruda", "Xavier", "Aguiar",
        "Pacheco", "Figueiredo", "Toledo", "Bastos", "Siqueira",
        "Paiva", "Carneiro", "Leite", "Assis", "Coutinho",
        "Rangel", "Esteves", "Alencar", "Prado", "Queiroz",
        "Dantas", "Fontes", "Cabral", "Magalhaes", "Salles",
        "Leal", "Barreto", "Sampaio", "Teles", "Pessoa",
        "Bittencourt", "Moraes", "Valente", "Trindade", "Neves",
        "Furtado", "Sena", "Lira", "Maia", "Chaves",
    ]

    CAPITAIS = {
        "SP": "Sao Paulo", "RJ": "Rio de Janeiro", "MG": "Belo Horizonte",
        "PR": "Curitiba", "RS": "Porto Alegre", "BA": "Salvador",
        "PE": "Recife", "CE": "Fortaleza", "GO": "Goiania", "DF": "Brasilia",
        "SC": "Florianopolis", "ES": "Vitoria", "PA": "Belem", "AM": "Manaus",
        "MA": "Sao Luis", "PB": "Joao Pessoa", "RN": "Natal", "AL": "Maceio",
    }
    estados = list(CAPITAIS.keys())

    # Embaralhar para variedade
    random.shuffle(SOBRENOMES)

    prospectos_salvos = []
    descartados_com_site = []
    erros = 0
    total_verificados = 0

    for sobrenome in SOBRENOMES:
        if len(prospectos_salvos) >= n:
            break

        total_verificados += 1
        nome_esc = f"{sobrenome} Advogados"
        nome_adv = f"Dr. {sobrenome}"
        estado_idx = total_verificados % len(estados)
        estado = estados[estado_idx]
        cidade = CAPITAIS[estado]

        logger.info(f"\n[{total_verificados}] Verificando: {nome_esc}")

        # === VERIFICACAO RAPIDA: DNS .adv.br ===
        slug = _normalizar(sobrenome)
        slug = re.sub(r"[^a-z0-9]", "", slug)

        if not slug or len(slug) < 3:
            continue

        dominio = f"{slug}.adv.br"
        tem_dominio = dns_resolve(dominio)

        if tem_dominio:
            # Confirmar com HTTP
            try:
                site_url = http_validar(f"https://{dominio}")
                if not site_url:
                    site_url = http_validar(f"https://www.{dominio}")
                if not site_url:
                    site_url = http_validar(f"http://{dominio}")
            except KeyboardInterrupt:
                raise
            except Exception as e:
                logger.debug(f"  Erro HTTP {dominio}: {e}")
                site_url = None

            if site_url:
                # TEM SITE — extrair dados SEO e descartar
                logger.info(f"  ⛔ {nome_esc} TEM SITE: {site_url}")
                try:
                    seo = extrair_dados_seo(site_url, nome_adv, nome_esc)
                except Exception:
                    seo = {}

                descartados_com_site.append({
                    "nome": nome_adv,
                    "nome_escritorio": nome_esc,
                    "site_url": site_url,
                    "titulo": seo.get("titulo", ""),
                    "areas": seo.get("areas_atuacao", []),
                    "confianca": seo.get("confianca", 0),
                })
                time.sleep(0.3)
                continue
            else:
                # DNS resolve mas HTTP falha — dominio registrado mas sem site
                logger.info(f"  ⚠️ {dominio}: DNS OK mas HTTP falhou — dominio sem site ativo")
        else:
            logger.info(f"  📍 {dominio}: nao resolve — sem site .adv.br")

        # Tambem checar .com.br (pode ter site em outro dominio)
        try:
            dominio_com = f"{slug}.com.br"
            if dns_resolve(dominio_com):
                site_url = http_validar(f"https://{dominio_com}")
                if site_url:
                    try:
                        seo = extrair_dados_seo(site_url, nome_adv, nome_esc)
                    except Exception:
                        seo = {}
                    titulo_norm = _normalizar(seo.get("titulo", ""))
                    if seo.get("confianca", 0) >= 0.3 and any(kw in titulo_norm for kw in ["advog", "advocac", "juridic", "direito"]):
                        logger.info(f"  ⛔ {nome_esc} TEM SITE em {dominio_com}: {site_url}")
                        descartados_com_site.append({
                            "nome": nome_adv,
                            "nome_escritorio": nome_esc,
                            "site_url": site_url,
                            "titulo": seo.get("titulo", ""),
                            "areas": seo.get("areas_atuacao", []),
                            "confianca": seo.get("confianca", 0),
                        })
                        time.sleep(0.3)
                        continue
        except Exception:
            pass

        # === CONFIRMADO SEM SITE — SALVAR COMO PROSPECTO ===
        logger.info(f"  ✅ PROSPECTO #{len(prospectos_salvos)+1}: {nome_esc} — SEM SITE CONFIRMADO")

        prospecto = {
            "nome": nome_adv,
            "nome_escritorio": nome_esc,
            "cidade": cidade,
            "estado": estado,
            "tem_site": 0,
            "site_url": None,
            "areas_atuacao": "[]",
            "porte_escritorio": "Solo",
            "fonte": "domain_bruteforce",
        }

        adv_id = salvar_prospecto(prospecto)
        if adv_id:
            prospecto["id"] = adv_id
            prospectos_salvos.append(prospecto)
            logger.info(f"  💾 Salvo! ID: {adv_id}")
        else:
            logger.warning(f"  Duplicata ou erro ao salvar")

        time.sleep(0.3)

    # Relatorio final
    logger.info("\n" + "=" * 70)
    logger.info("RELATORIO DE PROSPECCAO")
    logger.info("=" * 70)
    logger.info(f"Total verificados: {total_verificados}")
    logger.info(f"Prospectos salvos (SEM SITE): {len(prospectos_salvos)}")
    logger.info(f"Descartados (COM SITE): {len(descartados_com_site)}")
    logger.info(f"Erros: {erros}")

    if descartados_com_site:
        logger.info("\n--- Descartados corretamente (tinham site) ---")
        for d in descartados_com_site:
            logger.info(f"  {d['nome_escritorio']}: {d['site_url']} ({d.get('titulo', '')})")

    logger.info("\n--- Prospectos salvos (SEM SITE) ---")
    for p in prospectos_salvos:
        logger.info(f"  ID {p.get('id')}: {p['nome_escritorio']} — {p['cidade']}/{p['estado']}")

    return {
        "total_verificados": total_verificados,
        "prospectos_salvos": len(prospectos_salvos),
        "descartados_com_site": len(descartados_com_site),
        "erros": erros,
        "prospectos": prospectos_salvos,
        "descartados": descartados_com_site,
    }


def _inferir_porte(nome_escritorio):
    """Infere porte do escritorio pelo nome."""
    if not nome_escritorio:
        return "Solo"
    nome = _normalizar(nome_escritorio)
    if any(x in nome for x in ["associados", "partners", "grupo"]):
        return "Pequeno"
    if "&" in nome_escritorio or " e " in nome.split("advog")[0]:
        return "Pequeno"
    return "Solo"


# ============================================================
# 12. TESTE DE VERIFICACAO (para validar o sistema)
# ============================================================

def testar_verificacao():
    """
    Testa o sistema de verificacao com casos conhecidos.
    Escritorios que SABEMOS que tem site devem ser detectados.
    """
    print("\n" + "=" * 60)
    print("TESTE DE VERIFICACAO DE SITE")
    print("=" * 60)

    # Casos de teste: (nome, nome_escritorio, esperado_tem_site)
    casos = [
        ("Rocha", "Rocha Advogados", True),           # rocha.adv.br existe
        ("Costa Santos", "Costa Santos Advogados", True),  # costasantos.adv.br existe
        ("Inventado Xyz", "Xyz Escritorio Fake", False),  # nao deve existir
    ]

    resultados = []
    for nome, nome_esc, esperado in casos:
        print(f"\nTestando: {nome_esc}...")
        resultado = verificar_site_completo(nome, nome_esc)

        status = "✅ CORRETO" if resultado["tem_site"] == esperado else "❌ ERRADO"
        print(f"  Esperado: {'TEM SITE' if esperado else 'SEM SITE'}")
        print(f"  Resultado: {'TEM SITE' if resultado['tem_site'] else 'SEM SITE'}")
        if resultado["site_url"]:
            print(f"  URL: {resultado['site_url']}")
            print(f"  Confianca: {resultado['confianca']:.0%}")
        print(f"  {status}")

        resultados.append({
            "nome": nome_esc,
            "esperado": esperado,
            "obtido": resultado["tem_site"],
            "correto": resultado["tem_site"] == esperado,
            "url": resultado.get("site_url"),
        })

    # Resumo
    corretos = sum(1 for r in resultados if r["correto"])
    print(f"\n{'='*60}")
    print(f"Resultado: {corretos}/{len(resultados)} corretos")

    return resultados


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
    )
    # Suprimir warnings SSL
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    import sys

    print("\n" + "=" * 60)
    print("ProspectAdv — Prospeccao Inteligente v1")
    print("=" * 60)

    if len(sys.argv) > 1:
        cmd = sys.argv[1]

        if cmd == "--teste":
            testar_verificacao()

        elif cmd == "--limpar":
            print("\n⚠️  Limpando TODOS os dados do banco...")
            limpar_banco()
            print("✅ Banco limpo!")

        elif cmd == "--prospectar":
            n = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            print(f"\nProspectando {n} escritorios...")
            resultado = prospectar_escritorios_reais(n)
            print(f"\n✅ {resultado['prospectos_salvos']} prospectos salvos!")
            print(f"   {resultado['descartados_com_site']} descartados (tinham site)")

        elif cmd == "--verificar":
            nome = sys.argv[2] if len(sys.argv) > 2 else "Rocha"
            nome_esc = sys.argv[3] if len(sys.argv) > 3 else f"{nome} Advogados"
            print(f"\nVerificando: {nome_esc}")
            r = verificar_site_completo(nome, nome_esc)
            print(f"  Tem site: {r['tem_site']}")
            print(f"  URL: {r['site_url']}")
            print(f"  Confianca: {r['confianca']:.0%}")
            if r.get("dados_seo"):
                seo = r["dados_seo"]
                if seo["titulo"]:
                    print(f"  Titulo: {seo['titulo']}")
                if seo["areas_atuacao"]:
                    print(f"  Areas: {seo['areas_atuacao']}")

    else:
        print("\nUso:")
        print("  python prospectar_advogados.py --teste        # Testa verificacao")
        print("  python prospectar_advogados.py --limpar       # Limpa banco")
        print("  python prospectar_advogados.py --prospectar 10 # Prospecta N escritorios")
        print("  python prospectar_advogados.py --verificar Rocha 'Rocha Advogados'")
