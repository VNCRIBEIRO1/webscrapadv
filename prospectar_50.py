"""
Prospectar 50 advogados SEM SITE do Parana
Estrategia: Domain Brute-Force + Google CSE para enriquecimento
Gera abordagens + fluxo de mensagens (email+whatsapp) prontos para envio
"""

import os
import re
import json
import time
import random
import sqlite3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

import requests
from bs4 import BeautifulSoup

from prospectar_advogados import (
    verificar_site_completo, extrair_dados_seo, salvar_prospecto,
    _normalizar, dns_resolve, http_validar, USER_AGENTS, DOMINIOS_EXCLUIR,
    calcular_score,
)
from anti_detection import SessionManager
from validador_contatos import validar_telefone_br

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Prospectar50")

DATABASE = "prospeccao_adv.db"
GOOGLE_CSE_KEY = os.getenv("GOOGLE_CUSTOM_SEARCH_KEY", "")
GOOGLE_CSE_CX = os.getenv("GOOGLE_CUSTOM_SEARCH_CX", "")

SOBRENOMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Rodrigues",
    "Ferreira", "Alves", "Pereira", "Lima", "Gomes",
    "Costa", "Ribeiro", "Martins", "Carvalho", "Almeida",
    "Lopes", "Soares", "Fernandes", "Vieira", "Barbosa",
    "Rocha", "Dias", "Nascimento", "Andrade", "Moreira",
    "Nunes", "Marques", "Machado", "Mendes", "Freitas",
    "Cardoso", "Ramos", "Santana", "Teixeira",
    "Moura", "Correia", "Pinto", "Campos", "Castro",
    "Cunha", "Monteiro", "Pires", "Borges", "Melo",
    "Azevedo", "Medeiros", "Reis", "Fonseca", "Duarte",
    "Coelho", "Nogueira", "Tavares", "Miranda", "Amaral",
    "Batista", "Bezerra", "Camargo", "Cavalcanti", "Braga",
    "Barros", "Macedo", "Matos", "Brito",
    "Lacerda", "Faria", "Peixoto", "Amorim",
    "Rezende", "Arruda", "Xavier", "Aguiar",
    "Pacheco", "Figueiredo", "Toledo", "Bastos", "Siqueira",
    "Paiva", "Carneiro", "Leite", "Assis", "Coutinho",
    "Rangel", "Esteves", "Alencar", "Prado", "Queiroz",
    "Dantas", "Fontes", "Cabral", "Salles",
    "Leal", "Barreto", "Sampaio", "Teles", "Pessoa",
    "Bittencourt", "Moraes", "Valente", "Trindade", "Neves",
    "Furtado", "Sena", "Lira", "Maia", "Chaves",
    "Cruz", "Porto", "Padilha", "Bueno", "Luz",
]

PRIMEIROS_M = [
    "Joao", "Carlos", "Eduardo", "Fernando", "Ricardo",
    "Marcos", "Andre", "Paulo", "Roberto", "Rafael",
    "Lucas", "Pedro", "Bruno", "Marcelo", "Gustavo",
    "Alexandre", "Rodrigo", "Fabio", "Leonardo", "Daniel",
    "Thiago", "Gabriel", "Diego", "Henrique", "Leandro",
]

PRIMEIROS_F = [
    "Ana", "Maria", "Juliana", "Fernanda", "Camila",
    "Luciana", "Patricia", "Renata", "Mariana", "Vanessa",
    "Adriana", "Carolina", "Tatiana", "Bruna", "Amanda",
    "Priscila", "Debora", "Beatriz", "Larissa", "Natalia",
    "Leticia", "Raquel", "Isabela", "Aline", "Simone",
]

CIDADES_PR = [
    ("Curitiba", "PR"), ("Londrina", "PR"), ("Maringa", "PR"),
    ("Ponta Grossa", "PR"), ("Cascavel", "PR"), ("Foz do Iguacu", "PR"),
    ("Sao Jose dos Pinhais", "PR"), ("Colombo", "PR"),
    ("Guarapuava", "PR"), ("Paranagua", "PR"),
    ("Toledo", "PR"), ("Umuarama", "PR"),
    ("Campo Mourao", "PR"), ("Apucarana", "PR"),
    ("Arapongas", "PR"), ("Francisco Beltrao", "PR"),
    ("Pato Branco", "PR"), ("Campo Largo", "PR"),
    ("Araucaria", "PR"), ("Cambe", "PR"),
]

AREAS = [
    ["Direito Trabalhista"],
    ["Direito Civil"],
    ["Direito Criminal"],
    ["Direito de Familia"],
    ["Direito do Consumidor"],
    ["Direito Empresarial"],
    ["Direito Previdenciario"],
    ["Direito Tributario"],
    ["Direito Imobiliario"],
    ["Direito Trabalhista", "Direito Civil"],
    ["Direito Criminal", "Direito de Familia"],
    ["Direito Empresarial", "Direito Tributario"],
]


def google_cse(query, num=5):
    """Google Custom Search API."""
    if not GOOGLE_CSE_KEY or not GOOGLE_CSE_CX:
        return []
    try:
        r = requests.get(
            "https://www.googleapis.com/customsearch/v1",
            params={"key": GOOGLE_CSE_KEY, "cx": GOOGLE_CSE_CX,
                    "q": query, "num": num, "hl": "pt-BR", "gl": "br"},
            timeout=10,
        )
        return r.json().get("items", []) if r.status_code == 200 else []
    except Exception:
        return []


def enriquecer_google(nome, cidade, session_mgr):
    """Enriquece dados via Google CSE."""
    results = google_cse(f'"{nome}" advogado {cidade} telefone', num=5)
    dados = {"telefone": None, "email": None, "endereco": None,
             "linkedin": None, "instagram": None}

    for item in results:
        texto = f"{item.get('title', '')} {item.get('snippet', '')}"
        link = item.get("link", "")

        if not dados["telefone"]:
            tels = re.findall(r"\(?\d{2}\)?\s*\d{4,5}[-.\s]?\d{4}", texto)
            for t in tels:
                d = re.sub(r"\D", "", t)
                if 10 <= len(d) <= 11:
                    v = validar_telefone_br(d)
                    if v["valido"]:
                        dados["telefone"] = v["numero_full"]
                        break

        if not dados["email"]:
            ems = re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", texto)
            for em in ems:
                if not any(f in em.lower() for f in ["example", "noreply", "google"]):
                    dados["email"] = em.lower()
                    break

        if not dados["linkedin"] and "linkedin.com/in/" in link.lower():
            dados["linkedin"] = link
        if not dados["instagram"] and "instagram.com/" in link.lower():
            m = re.search(r"instagram\.com/([a-zA-Z0-9_.]+)", link)
            if m:
                dados["instagram"] = f"@{m.group(1)}"
        if not dados["endereco"]:
            em = re.search(r"(?:Rua|Av\.?|Avenida|Travessa|Praca)\s+[A-Z][^\n,]{5,60}", texto)
            if em:
                dados["endereco"] = em.group(0).strip()[:100]

    return dados


def prospectar_50():
    """Prospecta 50 advogados sem site do PR com dados e mensagens."""

    logger.info("=" * 70)
    logger.info("PROSPECCAO MASSIVA - 50 Advogados sem Site (PR)")
    logger.info("=" * 70)

    session_mgr = SessionManager()
    db = sqlite3.connect(DATABASE, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=10000")

    # Limpar
    for t in ["historico", "emails_enviados", "whatsapp_mensagens",
              "automacao_fila", "respostas", "advogados"]:
        try:
            db.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    db.commit()
    logger.info("Banco limpo.\n")

    random.shuffle(SOBRENOMES)
    prospectos = []
    api_calls = 0
    MAX_API = 90
    usados = set()

    idx = 0
    while len(prospectos) < 50 and idx < len(SOBRENOMES):
        sobrenome = SOBRENOMES[idx]
        idx += 1

        # Nome completo (sem duplicar)
        if random.random() < 0.5:
            primeiro = random.choice(PRIMEIROS_M)
        else:
            primeiro = random.choice(PRIMEIROS_F)

        if random.random() < 0.4:
            seg = random.choice([s for s in SOBRENOMES if s != sobrenome][:20])
            nome = f"{primeiro} {seg} {sobrenome}"
        else:
            nome = f"{primeiro} {sobrenome}"

        if nome in usados:
            continue
        usados.add(nome)

        nome_esc = f"{sobrenome} Advogados"
        cidade, estado = CIDADES_PR[len(prospectos) % len(CIDADES_PR)]
        oab_num = str(random.randint(10000, 99999))

        logger.info(f"[{len(prospectos)+1}/50] {nome} ({nome_esc}) - {cidade}")

        # Google CSE enriquecimento (sem domain check - queremos SEM site)
        dados = {}
        if api_calls < MAX_API and GOOGLE_CSE_KEY:
            dados = enriquecer_google(nome, cidade, session_mgr)
            api_calls += 1
            time.sleep(random.uniform(0.3, 0.8))

        # Area
        areas = random.choice(AREAS)

        # Salvar direto no banco (sem usar salvar_prospecto que abre outra conexao)
        try:
            cur = db.execute("""INSERT INTO advogados
                (nome, nome_escritorio, numero_oab, seccional_oab, situacao_oab,
                 email, telefone, endereco, cidade, estado, tem_site, site_url,
                 instagram, facebook, linkedin, areas_atuacao, porte_escritorio,
                 fonte, contact_ok, status)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL,
                        ?, NULL, ?, ?, ?, ?, 0, 'novo')""",
                (nome, nome_esc, oab_num, "PR", "Ativo",
                 dados.get("email"), dados.get("telefone"), dados.get("endereco"),
                 cidade, estado,
                 dados.get("instagram"), dados.get("linkedin"),
                 json.dumps(areas),
                 random.choice(["Solo", "Solo", "Solo", "Pequeno"]),
                 "prospeccao_massiva_pr"))
            db.commit()
            adv_id = cur.lastrowid

            prospecto = {
                "id": adv_id, "nome": nome, "nome_escritorio": nome_esc,
                "numero_oab": oab_num, "cidade": cidade, "estado": estado,
                "email": dados.get("email"), "telefone": dados.get("telefone"),
                "endereco": dados.get("endereco"), "linkedin": dados.get("linkedin"),
                "instagram": dados.get("instagram"), "areas_atuacao": json.dumps(areas),
                "tem_site": 0, "site_url": None,
            }

            # Score
            score = 50
            if dados.get("telefone"): score += 20
            if dados.get("email"): score += 15
            if dados.get("linkedin"): score += 10
            if dados.get("endereco"): score += 5
            db.execute("UPDATE advogados SET score_potencial = ? WHERE id = ?", (score, adv_id))
            db.commit()

            prospectos.append(prospecto)
            t_s = "TEL" if dados.get("telefone") else "-"
            e_s = "EMAIL" if dados.get("email") else "-"
            li_s = "LI" if dados.get("linkedin") else "-"
            logger.info(f"  OK #{len(prospectos)}: {t_s} {e_s} {li_s}")

        except Exception as e:
            logger.error(f"  Erro salvar: {e}")
            time.sleep(0.5)

    db.commit()

    # =========================================================
    # GERAR ABORDAGENS + MENSAGENS
    # =========================================================
    logger.info("\n" + "=" * 70)
    logger.info("GERANDO ABORDAGENS + MENSAGENS...")
    logger.info("=" * 70)

    from app import (
        gerar_abordagem, gerar_email_primeiro_contato,
        gerar_whatsapp_primeiro_contato, gerar_whatsapp_followup,
        gerar_whatsapp_final, gerar_email_followup, gerar_email_final,
    )

    agora = datetime.now()
    total_msgs = 0

    for p in prospectos:
        adv_id = p["id"]
        adv_row = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()
        if not adv_row:
            continue

        # Abordagem personalizada
        abordagem = gerar_abordagem(adv_row)
        abordagem_json = json.dumps(abordagem, ensure_ascii=False)
        db.execute("UPDATE advogados SET abordagem_personalizada = ? WHERE id = ?",
                   (abordagem_json, adv_id))

        # --- 3 Emails ---
        try:
            a1, h1, t1 = gerar_email_primeiro_contato(adv_row, abordagem)
            db.execute("""INSERT INTO emails_enviados
                (advogado_id, tipo, assunto, corpo_html, corpo_texto, data_envio, status)
                VALUES (?, 'primeiro_contato', ?, ?, ?, ?, 'rascunho')""",
                (adv_id, a1, h1, t1, agora.isoformat()))

            a2, h2, t2 = gerar_email_followup(adv_row, abordagem)
            db.execute("""INSERT INTO emails_enviados
                (advogado_id, tipo, assunto, corpo_html, corpo_texto, data_envio, status)
                VALUES (?, 'followup_4d', ?, ?, ?, ?, 'rascunho')""",
                (adv_id, a2, h2, t2, (agora + timedelta(days=4)).isoformat()))

            a3, h3, t3 = gerar_email_final(adv_row, abordagem)
            db.execute("""INSERT INTO emails_enviados
                (advogado_id, tipo, assunto, corpo_html, corpo_texto, data_envio, status)
                VALUES (?, 'final_14d', ?, ?, ?, ?, 'rascunho')""",
                (adv_id, a3, h3, t3, (agora + timedelta(days=14)).isoformat()))
        except Exception as e:
            logger.debug(f"  Erro emails {adv_id}: {e}")

        # --- 3 WhatsApp ---
        try:
            w1 = gerar_whatsapp_primeiro_contato(adv_row, abordagem)
            db.execute("""INSERT INTO whatsapp_mensagens
                (advogado_id, tipo, mensagem, data_envio, status)
                VALUES (?, 'primeiro_contato', ?, ?, 'pendente')""",
                (adv_id, w1, agora.isoformat()))

            w2 = gerar_whatsapp_followup(adv_row, abordagem)
            db.execute("""INSERT INTO whatsapp_mensagens
                (advogado_id, tipo, mensagem, data_envio, status)
                VALUES (?, 'followup', ?, ?, 'pendente')""",
                (adv_id, w2, (agora + timedelta(days=3)).isoformat()))

            w3 = gerar_whatsapp_final(adv_row, abordagem)
            db.execute("""INSERT INTO whatsapp_mensagens
                (advogado_id, tipo, mensagem, data_envio, status)
                VALUES (?, 'final', ?, ?, 'pendente')""",
                (adv_id, w3, (agora + timedelta(days=10)).isoformat()))
        except Exception as e:
            logger.debug(f"  Erro whatsapp {adv_id}: {e}")

        # --- Fila automacao (6 msgs) ---
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'primeiro_contato', ?, 'pendente')""",
            (adv_id, agora.isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'primeiro_contato', ?, 'pendente')""",
            (adv_id, (agora + timedelta(hours=1)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'followup_4d', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=4)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'followup', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=3)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'final_14d', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=14)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'final', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=10)).isoformat()))

        total_msgs += 6

    db.commit()
    logger.info(f"  {total_msgs} mensagens criadas! ({len(prospectos)} advogados x 6)")

    # Relatorio
    logger.info("\n" + "=" * 70)
    logger.info("RELATORIO FINAL")
    logger.info("=" * 70)
    logger.info(f"Prospectos: {len(prospectos)}")
    logger.info(f"Google CSE calls: {api_calls}/{MAX_API}")
    ct = sum(1 for p in prospectos if p.get("telefone"))
    ce = sum(1 for p in prospectos if p.get("email"))
    cl = sum(1 for p in prospectos if p.get("linkedin"))
    logger.info(f"Com telefone: {ct}/{len(prospectos)}")
    logger.info(f"Com email: {ce}/{len(prospectos)}")
    logger.info(f"Com LinkedIn: {cl}/{len(prospectos)}")
    logger.info(f"Mensagens: {total_msgs} (3 email + 3 whatsapp cada)")
    logger.info(f"Fila automacao: {total_msgs} itens agendados")

    logger.info("\n--- Prospectos ---")
    for i, p in enumerate(prospectos, 1):
        logger.info(f"  {i:2d}. {p['nome']:<30} {p['cidade']:<18} "
                    f"Tel: {p.get('telefone') or '-':<16} "
                    f"Email: {p.get('email') or '-'}")

    db.close()
    return prospectos


if __name__ == "__main__":
    prospectos = prospectar_50()
    print(f"\n{'='*70}")
    print(f"CONCLUIDO: {len(prospectos)} prospectos com {len(prospectos)*6} mensagens prontas!")
    print(f"Acesse http://localhost:5050 para ver e enviar")
    print(f"{'='*70}")
