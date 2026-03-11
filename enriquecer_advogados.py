"""
Enriquecer Advogados — Adiciona dados complementares e calcula scores
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

logger = logging.getLogger("ProspectAdv.Enriquecer")

DATABASE = "prospeccao_adv.db"
GOOGLE_MAPS_API_KEY = os.getenv("GOOGLE_MAPS_API_KEY", "")


def _get_db():
    """Retorna conexão SQLite."""
    db = sqlite3.connect(DATABASE)
    db.row_factory = sqlite3.Row
    return db


def calcular_score(adv):
    """Calcula score de potencial (0-100)."""
    score = 0

    # SEM SITE = principal critério (+30)
    if not adv["tem_site"]:
        score += 30

    # Presença digital parcial (+20)
    redes = 0
    if adv["instagram"]:
        redes += 1
    if adv["facebook"]:
        redes += 1
    if adv["linkedin"]:
        redes += 1
    if redes >= 1 and not adv["tem_site"]:
        score += min(redes * 7, 20)

    # Google Maps com reviews (+15)
    if adv["google_avaliacao"] and adv["google_avaliacao"] >= 4.0:
        score += 10
    if adv["google_reviews"] and adv["google_reviews"] >= 5:
        score += 5

    # Tempo de atuação (+10)
    if adv["tempo_atuacao"]:
        if adv["tempo_atuacao"] >= 5:
            score += 10
        elif adv["tempo_atuacao"] >= 2:
            score += 5

    # Volume de processos (+10)
    if adv["volume_processos"] and adv["volume_processos"] >= 10:
        score += 10
    elif adv["volume_processos"] and adv["volume_processos"] >= 5:
        score += 5

    # Porte do escritório (+10)
    if adv["porte_escritorio"] in ("Pequeno", "Médio"):
        score += 10
    elif adv["porte_escritorio"] == "Solo":
        score += 5

    # Múltiplas áreas (+5)
    try:
        areas = json.loads(adv["areas_atuacao"] or "[]")
    except (json.JSONDecodeError, TypeError):
        areas = []
    if len(areas) >= 3:
        score += 5

    return min(score, 100)


def inferir_areas_por_nome(nome_escritorio):
    """Infere áreas de atuação com base no nome do escritório."""
    if not nome_escritorio:
        return []

    nome = nome_escritorio.lower()
    areas = []

    mapeamento = {
        "trabalhist": "Direito Trabalhista",
        "trabalho": "Direito Trabalhista",
        "criminal": "Direito Criminal",
        "penal": "Direito Criminal",
        "civil": "Direito Civil",
        "consumidor": "Direito do Consumidor",
        "consumerista": "Direito do Consumidor",
        "empresarial": "Direito Empresarial",
        "societário": "Direito Empresarial",
        "societario": "Direito Empresarial",
        "família": "Direito de Família",
        "familia": "Direito de Família",
        "divórcio": "Direito de Família",
        "divorcio": "Direito de Família",
        "previdenciário": "Direito Previdenciário",
        "previdenciario": "Direito Previdenciário",
        "inss": "Direito Previdenciário",
        "tributário": "Direito Tributário",
        "tributario": "Direito Tributário",
        "fiscal": "Direito Tributário",
        "imobiliário": "Direito Imobiliário",
        "imobiliario": "Direito Imobiliário",
        "ambiental": "Direito Ambiental",
        "digital": "Direito Digital",
        "tecnologia": "Direito Digital",
        "médico": "Direito Médico",
        "saúde": "Direito Médico",
    }

    for keyword, area in mapeamento.items():
        if keyword in nome and area not in areas:
            areas.append(area)

    return areas


def inferir_porte(nome_escritorio):
    """Infere porte do escritório pelo nome."""
    if not nome_escritorio:
        return "Solo"

    nome = nome_escritorio.lower()

    if any(x in nome for x in ["& associados", "associados", "& partners"]):
        return "Pequeno"
    if any(x in nome for x in ["grupo", "holding"]):
        return "Médio"
    if "&" in nome:
        return "Pequeno"

    return "Solo"


def buscar_email_google(nome, cidade, estado):
    """
    Tenta encontrar email do advogado via busca Google.
    NOTA: Respeitar rate limits e termos de uso.
    """
    query = f'"{nome}" advogado {cidade} {estado} email "@"'

    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        response = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=5",
            headers=headers,
            timeout=10,
        )

        if response.status_code == 200:
            # Extrair emails do HTML
            emails = re.findall(
                r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}",
                response.text,
            )

            # Filtrar emails genéricos
            emails_validos = [
                e for e in emails
                if not any(x in e.lower() for x in [
                    "google", "gstatic", "example", "sentry",
                    "schema.org", "w3.org", "googleapis",
                ])
            ]

            if emails_validos:
                return emails_validos[0]

    except Exception as e:
        logger.debug(f"Erro ao buscar email: {e}")

    return None


def buscar_redes_sociais(nome, cidade):
    """Busca redes sociais do advogado via Google."""
    resultados = {"instagram": None, "facebook": None, "linkedin": None}

    # Instagram
    try:
        query = f'site:instagram.com "{nome}" advogado {cidade}'
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

        response = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=3",
            headers=headers,
            timeout=10,
        )

        if response.status_code == 200:
            match = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", response.text)
            if match:
                username = match.group(1)
                if username not in ["explore", "accounts", "p"]:
                    resultados["instagram"] = f"@{username}"

        time.sleep(1)

    except Exception:
        pass

    # LinkedIn
    try:
        query = f'site:linkedin.com/in "{nome}" advogado {cidade}'
        response = requests.get(
            f"https://www.google.com/search?q={quote_plus(query)}&num=3",
            headers=headers,
            timeout=10,
        )

        if response.status_code == 200:
            match = re.search(r"linkedin\.com/in/([a-zA-Z0-9_-]+)", response.text)
            if match:
                resultados["linkedin"] = match.group(1)

        time.sleep(1)

    except Exception:
        pass

    return resultados


def enriquecer_advogado(adv_id, buscar_email=True, buscar_redes=True):
    """
    Enriquece dados de um advogado específico.
    
    Args:
        adv_id: ID do advogado no banco
        buscar_email: Tentar encontrar email
        buscar_redes: Tentar encontrar redes sociais
    """
    db = _get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()

    if not adv:
        logger.warning(f"Advogado {adv_id} não encontrado")
        return

    atualizacoes = {}

    # Inferir áreas de atuação se não existir
    if not adv["areas_atuacao"] or adv["areas_atuacao"] == "[]":
        areas = inferir_areas_por_nome(adv["nome_escritorio"] or adv["nome"])
        if areas:
            atualizacoes["areas_atuacao"] = json.dumps(areas, ensure_ascii=False)

    # Inferir porte se não existir
    if not adv["porte_escritorio"]:
        porte = inferir_porte(adv["nome_escritorio"])
        atualizacoes["porte_escritorio"] = porte

    # Buscar email
    if buscar_email and not adv["email"] and adv["cidade"]:
        email = buscar_email_google(adv["nome"], adv["cidade"], adv["estado"])
        if email:
            atualizacoes["email"] = email
            logger.info(f"Email encontrado para {adv['nome']}: {email}")
        time.sleep(2)

    # Buscar redes sociais
    if buscar_redes and adv["cidade"]:
        if not adv["instagram"] or not adv["linkedin"]:
            redes = buscar_redes_sociais(adv["nome"], adv["cidade"])
            if redes["instagram"] and not adv["instagram"]:
                atualizacoes["instagram"] = redes["instagram"]
            if redes["linkedin"] and not adv["linkedin"]:
                atualizacoes["linkedin"] = redes["linkedin"]
            time.sleep(2)

    # Aplicar atualizações
    if atualizacoes:
        sets = ", ".join(f"{k} = ?" for k in atualizacoes.keys())
        values = list(atualizacoes.values()) + [adv_id]
        db.execute(f"UPDATE advogados SET {sets} WHERE id = ?", values)

    # Recalcular score
    adv_atualizado = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()
    score = calcular_score(dict(adv_atualizado))
    db.execute("UPDATE advogados SET score_potencial = ? WHERE id = ?", (score, adv_id))

    db.commit()
    db.close()

    logger.info(f"Advogado {adv_id} enriquecido — Score: {score}")
    return score


def enriquecer_todos(limite=100, buscar_email=True, buscar_redes=True):
    """
    Enriquece dados de todos os advogados com score baixo ou sem dados.
    
    Args:
        limite: Máximo de advogados para processar
        buscar_email: Tentar encontrar emails
        buscar_redes: Tentar encontrar redes sociais
    """
    db = _get_db()

    advogados = db.execute("""
        SELECT id FROM advogados
        WHERE tem_site = 0
          AND (
            score_potencial = 0
            OR areas_atuacao IS NULL
            OR areas_atuacao = '[]'
            OR porte_escritorio IS NULL
          )
        ORDER BY data_criacao DESC
        LIMIT ?
    """, (limite,)).fetchall()

    db.close()

    total = len(advogados)
    logger.info(f"Enriquecendo {total} advogados...")

    for i, adv in enumerate(advogados, 1):
        try:
            enriquecer_advogado(
                adv["id"],
                buscar_email=buscar_email,
                buscar_redes=buscar_redes,
            )
            logger.info(f"[{i}/{total}] Advogado {adv['id']} processado")
        except Exception as e:
            logger.error(f"Erro ao enriquecer advogado {adv['id']}: {e}")

        time.sleep(1)  # Rate limiting

    logger.info(f"Enriquecimento concluído: {total} advogados processados")


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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    print("ProspectAdv — Enriquecimento de Dados")
    print("=" * 50)

    # Recalcular scores
    total = recalcular_todos_scores()
    print(f"✅ Scores recalculados para {total} advogados")

    # Enriquecer dados
    print("\nIniciando enriquecimento de dados...")
    enriquecer_todos(limite=50, buscar_email=False, buscar_redes=False)
    print("✅ Enriquecimento concluído")
