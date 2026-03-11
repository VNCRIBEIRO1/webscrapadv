"""
Scraper de Advogados — Google Maps, OAB CNA, Redes Sociais
ProspectAdv
"""

import os
import re
import json
import time
import sqlite3
import logging
import requests
from datetime import datetime
from urllib.parse import quote_plus

from bs4 import BeautifulSoup

logger = logging.getLogger("ProspectAdv.Scraper")

GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")
DATABASE = "prospeccao_adv.db"

# Cidades prioritárias para scraping
CIDADES_PRIORITARIAS = [
    # Tier 1 — Capitais grandes
    ("São Paulo", "SP"), ("Rio de Janeiro", "RJ"), ("Belo Horizonte", "MG"),
    ("Curitiba", "PR"), ("Porto Alegre", "RS"), ("Salvador", "BA"),
    ("Brasília", "DF"), ("Fortaleza", "CE"), ("Recife", "PE"), ("Goiânia", "GO"),
    # Tier 2 — Capitais médias
    ("Manaus", "AM"), ("Belém", "PA"), ("Florianópolis", "SC"), ("Vitória", "ES"),
    ("Campo Grande", "MS"), ("Natal", "RN"), ("São Luís", "MA"), ("Maceió", "AL"),
    ("João Pessoa", "PB"), ("Aracaju", "SE"), ("Teresina", "PI"), ("Cuiabá", "MT"),
    # Tier 3 — Cidades médias (interior forte)
    ("Campinas", "SP"), ("Ribeirão Preto", "SP"), ("Santos", "SP"), ("Sorocaba", "SP"),
    ("Presidente Prudente", "SP"), ("Londrina", "PR"), ("Maringá", "PR"),
    ("Joinville", "SC"), ("Caxias do Sul", "RS"), ("Uberlândia", "MG"),
    ("Juiz de Fora", "MG"), ("Feira de Santana", "BA"),
]

# Termos de busca
TERMOS_BUSCA = [
    "escritório de advocacia",
    "advogado",
    "advocacia",
]


def _get_db():
    """Retorna conexão SQLite."""
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    return db


def _advogado_existe(db, nome, cidade):
    """Verifica se o advogado já existe no banco."""
    row = db.execute(
        "SELECT id FROM advogados WHERE nome = ? AND cidade = ?",
        (nome, cidade),
    ).fetchone()
    return row is not None


def _limpar_telefone(telefone):
    """Extrai apenas dígitos do telefone."""
    if not telefone:
        return None
    return "".join(filter(str.isdigit, telefone))


# ─── Google Maps / Places API ──────────────────────────────

def buscar_google_maps(cidade, estado, termo="escritório de advocacia", max_resultados=60):
    """
    Busca escritórios de advocacia no Google Maps via Places API.
    Filtra APENAS os que NÃO possuem site.
    
    Args:
        cidade: Nome da cidade
        estado: UF do estado
        termo: Termo de busca
        max_resultados: Máximo de resultados por busca
    
    Returns:
        Lista de dicts com dados dos advogados sem site
    """
    if not GOOGLE_MAPS_API_KEY:
        logger.error("GOOGLE_MAPS_API_KEY não configurada")
        return []

    query = f"{termo} {cidade} {estado}"
    leads = []
    next_page_token = None
    total_buscados = 0

    while total_buscados < max_resultados:
        params = {
            "query": query,
            "key": GOOGLE_MAPS_API_KEY,
            "language": "pt-BR",
            "region": "br",
        }

        if next_page_token:
            params["pagetoken"] = next_page_token
            time.sleep(2)  # Google exige delay entre páginas

        try:
            response = requests.get(
                "https://maps.googleapis.com/maps/api/place/textsearch/json",
                params=params,
                timeout=15,
            )
            data = response.json()

            if data.get("status") != "OK":
                logger.warning(f"Google Maps API status: {data.get('status')} - {data.get('error_message', '')}")
                break

            for place in data.get("results", []):
                total_buscados += 1
                place_id = place.get("place_id")

                if not place_id:
                    continue

                # Buscar detalhes do local
                detalhes = _obter_detalhes_place(place_id)
                if not detalhes:
                    continue

                # FILTRO CRÍTICO: Só prospectar quem NÃO tem site
                website = detalhes.get("website")
                if website:
                    logger.debug(f"Descartado (tem site): {detalhes.get('name')} — {website}")
                    continue

                lead = {
                    "nome": detalhes.get("name", ""),
                    "nome_escritorio": detalhes.get("name", ""),
                    "telefone": detalhes.get("formatted_phone_number"),
                    "whatsapp": _limpar_telefone(detalhes.get("international_phone_number")),
                    "endereco": detalhes.get("formatted_address", ""),
                    "cidade": cidade,
                    "estado": estado,
                    "tem_site": 0,
                    "google_maps_url": detalhes.get("url", ""),
                    "google_avaliacao": detalhes.get("rating"),
                    "google_reviews": detalhes.get("user_ratings_total", 0),
                    "foto": (
                        detalhes.get("photos", [{}])[0].get("photo_reference", "")
                        if detalhes.get("photos") else ""
                    ),
                    "fonte": "google_maps",
                }

                leads.append(lead)
                logger.info(f"Lead encontrado: {lead['nome']} — {cidade}/{estado} (sem site ✓)")

                # Rate limiting
                time.sleep(0.3)

            next_page_token = data.get("next_page_token")
            if not next_page_token:
                break

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro na busca Google Maps: {e}")
            break

    logger.info(f"Busca '{termo}' em {cidade}/{estado}: {len(leads)} leads sem site de {total_buscados} total")
    return leads


def _obter_detalhes_place(place_id):
    """Obtém detalhes completos de um local via Place Details API."""
    try:
        response = requests.get(
            "https://maps.googleapis.com/maps/api/place/details/json",
            params={
                "place_id": place_id,
                "key": GOOGLE_MAPS_API_KEY,
                "language": "pt-BR",
                "fields": (
                    "name,formatted_address,formatted_phone_number,"
                    "international_phone_number,website,url,rating,"
                    "user_ratings_total,photos,opening_hours,types"
                ),
            },
            timeout=10,
        )
        data = response.json()

        if data.get("status") == "OK":
            return data.get("result", {})
        return None

    except Exception as e:
        logger.error(f"Erro ao obter detalhes place {place_id}: {e}")
        return None


def salvar_leads_google_maps(leads):
    """Salva leads do Google Maps no banco de dados."""
    db = _get_db()
    inseridos = 0
    duplicados = 0

    for lead in leads:
        if _advogado_existe(db, lead["nome"], lead["cidade"]):
            duplicados += 1
            continue

        try:
            db.execute("""
                INSERT INTO advogados (
                    nome, nome_escritorio, telefone, whatsapp,
                    endereco, cidade, estado, tem_site,
                    google_maps_url, google_avaliacao, google_reviews,
                    foto, fonte, areas_atuacao
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                lead["nome"], lead["nome_escritorio"],
                lead.get("telefone"), lead.get("whatsapp"),
                lead.get("endereco"), lead["cidade"], lead["estado"],
                lead["tem_site"],
                lead.get("google_maps_url"), lead.get("google_avaliacao"),
                lead.get("google_reviews", 0),
                lead.get("foto", ""), lead["fonte"],
                '[]',  # Áreas serão enriquecidas depois
            ))
            inseridos += 1
        except Exception as e:
            logger.error(f"Erro ao inserir lead {lead['nome']}: {e}")

    db.commit()
    db.close()

    logger.info(f"Leads salvos: {inseridos} inseridos, {duplicados} duplicados")
    return {"inseridos": inseridos, "duplicados": duplicados}


def executar_scraping_google_maps(cidades=None, termos=None):
    """
    Executa scraping completo do Google Maps para as cidades prioritárias.
    
    Args:
        cidades: Lista de tuplas (cidade, estado) ou None para todas
        termos: Lista de termos de busca ou None para padrão
    """
    cidades = cidades or CIDADES_PRIORITARIAS
    termos = termos or TERMOS_BUSCA

    total_leads = 0
    resultados = {}

    for cidade, estado in cidades:
        resultados[f"{cidade}/{estado}"] = {"leads": 0, "detalhes": []}

        for termo in termos:
            logger.info(f"Buscando: '{termo}' em {cidade}/{estado}...")

            leads = buscar_google_maps(cidade, estado, termo)
            resultado = salvar_leads_google_maps(leads)

            total_leads += resultado["inseridos"]
            resultados[f"{cidade}/{estado}"]["leads"] += resultado["inseridos"]
            resultados[f"{cidade}/{estado}"]["detalhes"].append({
                "termo": termo,
                "encontrados": len(leads),
                "inseridos": resultado["inseridos"],
                "duplicados": resultado["duplicados"],
            })

            # Rate limiting entre buscas
            time.sleep(2)

        # Rate limiting entre cidades
        time.sleep(5)

    logger.info(f"Scraping concluído: {total_leads} leads totais inseridos")
    return {
        "total_leads": total_leads,
        "cidades_processadas": len(cidades),
        "resultados": resultados,
    }


# ─── OAB CNA Scraping ──────────────────────────────────────

def buscar_oab_cna(seccional="SP", nome="", pagina=1):
    """
    Busca advogados no Cadastro Nacional de Advogados da OAB.
    
    Args:
        seccional: UF da seccional (SP, RJ, MG, etc.)
        nome: Nome para filtrar (opcional)
        pagina: Número da página
    
    Returns:
        Lista de dicts com dados dos advogados
    """
    url = "https://cna.oab.org.br/search"

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json, text/javascript, */*",
            "Content-Type": "application/json",
            "Referer": "https://cna.oab.org.br/",
        }

        payload = {
            "IsMobile": False,
            "NomeAdvo": nome,
            "Inscricao": "",
            "Uf": seccional,
            "TipoInsc": "P",  # Principal
            "PageIndex": pagina,
            "PageSize": 20,
        }

        response = requests.post(
            url,
            json=payload,
            headers=headers,
            timeout=15,
        )

        if response.status_code != 200:
            logger.warning(f"OAB CNA retornou status {response.status_code}")
            return []

        data = response.json()
        advogados = []

        for item in data.get("Data", []):
            advogado = {
                "nome": item.get("Nome", "").strip(),
                "numero_oab": item.get("Inscricao", "").strip(),
                "seccional_oab": item.get("UF", seccional),
                "situacao_oab": item.get("TipoSituacao", ""),
                "tipo_inscricao": item.get("TipoInscricao", ""),
                "fonte": "oab_cna",
            }

            # Só incluir ativos
            if "ativo" in advogado["situacao_oab"].lower():
                advogados.append(advogado)

        logger.info(f"OAB CNA {seccional}: {len(advogados)} advogados ativos na página {pagina}")
        return advogados

    except Exception as e:
        logger.error(f"Erro ao buscar OAB CNA: {e}")
        return []


def salvar_leads_oab(advogados):
    """Salva leads da OAB no banco."""
    db = _get_db()
    inseridos = 0

    for adv in advogados:
        # Verificar se já existe pelo número OAB
        existe = db.execute(
            "SELECT id FROM advogados WHERE numero_oab = ? AND seccional_oab = ?",
            (adv["numero_oab"], adv["seccional_oab"]),
        ).fetchone()

        if existe:
            # Atualizar dados OAB de lead existente
            db.execute("""
                UPDATE advogados SET
                    situacao_oab = ?,
                    tipo_inscricao = ?
                WHERE id = ?
            """, (adv["situacao_oab"], adv["tipo_inscricao"], existe["id"]))
            continue

        try:
            db.execute("""
                INSERT INTO advogados (nome, numero_oab, seccional_oab, situacao_oab, tipo_inscricao, fonte, tem_site)
                VALUES (?, ?, ?, ?, ?, ?, 0)
            """, (
                adv["nome"], adv["numero_oab"], adv["seccional_oab"],
                adv["situacao_oab"], adv["tipo_inscricao"], adv["fonte"],
            ))
            inseridos += 1
        except Exception as e:
            logger.error(f"Erro ao inserir advogado OAB: {e}")

    db.commit()
    db.close()
    return inseridos


# ─── Instagram Scraping (básico — sem API oficial) ─────────

def buscar_instagram_advogados(termo="advogado", limite=50):
    """
    Busca perfis de advogados no Instagram.
    NOTA: Esta é uma versão simplificada. Para produção,
    use a Instagram Graph API ou serviços como Apify.
    
    Args:
        termo: Termo de busca
        limite: Máximo de perfis
    
    Returns:
        Lista de perfis encontrados
    """
    logger.warning(
        "Scraping direto do Instagram é limitado. "
        "Recomenda-se usar Instagram Graph API ou serviços como Apify."
    )

    # Placeholder — em produção, usar API oficial ou scraper externo
    perfis = []

    # Simulação de busca via hashtag/busca web
    keywords_bio = ["advogado", "advocacia", "OAB", "escritório", "direito"]

    try:
        # Busca via Google (Instagram profiles)
        query = f"site:instagram.com {termo} advogado OAB"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }

        response = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=20",
            headers=headers,
            timeout=10,
        )

        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")

            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "instagram.com" in href and "/p/" not in href:
                    # Extrair username
                    match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", href)
                    if match:
                        username = match.group(1)
                        if username not in ["explore", "accounts", "p", "stories", "reel"]:
                            perfis.append({
                                "instagram": f"@{username}",
                                "url": f"https://instagram.com/{username}",
                            })

        logger.info(f"Instagram: {len(perfis)} perfis encontrados para '{termo}'")

    except Exception as e:
        logger.error(f"Erro na busca Instagram: {e}")

    return perfis[:limite]


# ─── Scraping Completo ─────────────────────────────────────

def executar_scraping_completo(cidades=None, incluir_oab=True, incluir_instagram=False):
    """
    Executa scraping completo de todas as fontes.
    
    Args:
        cidades: Lista de (cidade, estado) ou None para todas
        incluir_oab: Incluir busca na OAB CNA
        incluir_instagram: Incluir busca no Instagram
    
    Returns:
        Relatório com total de leads por fonte
    """
    relatorio = {
        "inicio": datetime.now().isoformat(),
        "google_maps": {"total": 0, "status": "pendente"},
        "oab_cna": {"total": 0, "status": "pendente"},
        "instagram": {"total": 0, "status": "pendente"},
    }

    # 1. Google Maps (fonte principal)
    logger.info("=== Iniciando scraping Google Maps ===")
    try:
        resultado_gm = executar_scraping_google_maps(cidades)
        relatorio["google_maps"] = {
            "total": resultado_gm["total_leads"],
            "status": "concluído",
            "detalhes": resultado_gm,
        }
    except Exception as e:
        logger.error(f"Erro no scraping Google Maps: {e}")
        relatorio["google_maps"]["status"] = f"erro: {e}"

    # 2. OAB CNA
    if incluir_oab:
        logger.info("=== Iniciando scraping OAB CNA ===")
        seccionais = ["SP", "RJ", "MG", "RS", "PR", "BA", "PE", "CE", "GO", "DF"]
        total_oab = 0

        for sec in seccionais:
            try:
                for pagina in range(1, 6):  # 5 páginas por seccional
                    advogados = buscar_oab_cna(sec, pagina=pagina)
                    if not advogados:
                        break
                    inseridos = salvar_leads_oab(advogados)
                    total_oab += inseridos
                    time.sleep(2)  # Rate limiting
            except Exception as e:
                logger.error(f"Erro OAB CNA {sec}: {e}")

            time.sleep(5)

        relatorio["oab_cna"] = {"total": total_oab, "status": "concluído"}

    # 3. Instagram
    if incluir_instagram:
        logger.info("=== Iniciando scraping Instagram ===")
        try:
            perfis = buscar_instagram_advogados()
            relatorio["instagram"] = {
                "total": len(perfis),
                "status": "concluído",
                "perfis": perfis,
            }
        except Exception as e:
            logger.error(f"Erro no scraping Instagram: {e}")
            relatorio["instagram"]["status"] = f"erro: {e}"

    relatorio["fim"] = datetime.now().isoformat()
    relatorio["total_geral"] = sum(
        r.get("total", 0) for r in relatorio.values() if isinstance(r, dict) and "total" in r
    )

    logger.info(f"=== Scraping completo: {relatorio['total_geral']} leads ===")
    return relatorio


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("ProspectAdv — Scraper de Advogados")
    print("=" * 50)

    if not GOOGLE_MAPS_API_KEY:
        print("⚠️  GOOGLE_MAPS_API_KEY não configurada!")
        print("   Configure no arquivo .env ou variável de ambiente")
        print("   Executando apenas scraping OAB CNA...\n")

        resultado = executar_scraping_completo(
            cidades=CIDADES_PRIORITARIAS[:3],
            incluir_oab=True,
            incluir_instagram=False,
        )
    else:
        print(f"✅ Google Maps API Key configurada")
        print(f"   Buscando em {len(CIDADES_PRIORITARIAS)} cidades...\n")

        resultado = executar_scraping_completo(
            cidades=CIDADES_PRIORITARIAS[:5],  # Começar com 5 cidades
            incluir_oab=True,
            incluir_instagram=False,
        )

    print("\n" + "=" * 50)
    print(f"Total de leads: {resultado.get('total_geral', 0)}")
    print(json.dumps(resultado, indent=2, ensure_ascii=False, default=str))
