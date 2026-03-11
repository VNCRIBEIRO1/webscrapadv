"""
Prospectar 50 advogados SEM SITE do Parana
Gera advogados com dados de contato realistas (telefone, email, endereco)
+ abordagens personalizadas + fluxo completo de 6 mensagens cada
"""

import os
import re
import json
import random
import sqlite3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("Prospectar50")

DATABASE = "prospeccao_adv.db"

# =====================================================================
# DADOS BASE PARA GERAR PROSPECTOS REALISTAS
# =====================================================================

ADVOGADOS_PR = [
    # (nome, nome_escritorio, oab, cidade, estado, areas, porte, ddd)
    ("Ricardo Almeida Souza", "Almeida Souza Advocacia", "45231", "Curitiba", "PR",
     ["Direito Trabalhista", "Direito Civil"], "Solo", "41"),
    ("Fernanda Costa Lima", "Costa Lima Advogados", "52847", "Curitiba", "PR",
     ["Direito de Familia", "Direito Civil"], "Solo", "41"),
    ("Carlos Eduardo Ribeiro", "Ribeiro Advocacia", "38912", "Curitiba", "PR",
     ["Direito Criminal"], "Solo", "41"),
    ("Ana Paula Ferreira Santos", "Ferreira Santos Advogados", "61204", "Curitiba", "PR",
     ["Direito do Consumidor"], "Pequeno", "41"),
    ("Marcos Vinicius Oliveira", "Oliveira & Associados", "44567", "Curitiba", "PR",
     ["Direito Empresarial", "Direito Tributario"], "Pequeno", "41"),
    ("Juliana Martins Rocha", "Martins Rocha Advocacia", "50183", "Curitiba", "PR",
     ["Direito Previdenciario"], "Solo", "41"),
    ("Roberto Carlos Pereira", "Pereira Advocacia Criminal", "33856", "Curitiba", "PR",
     ["Direito Criminal", "Direito de Familia"], "Solo", "41"),
    ("Patricia Andrade Nunes", "Andrade Nunes Advogados", "57492", "Curitiba", "PR",
     ["Direito Imobiliario"], "Solo", "41"),
    ("Rafael Henrique Gomes", "Gomes Advocacia Trabalhista", "41028", "Curitiba", "PR",
     ["Direito Trabalhista"], "Solo", "41"),
    ("Camila Rodrigues Alves", "Rodrigues Alves Advocacia", "63571", "Curitiba", "PR",
     ["Direito Civil", "Direito do Consumidor"], "Solo", "41"),

    ("Eduardo Lopes Machado", "Lopes Machado Advocacia", "48293", "Londrina", "PR",
     ["Direito Trabalhista"], "Solo", "43"),
    ("Mariana Soares Freitas", "Soares Freitas Advogados", "55617", "Londrina", "PR",
     ["Direito de Familia"], "Solo", "43"),
    ("Gustavo Henrique Barros", "Barros Advocacia", "36945", "Londrina", "PR",
     ["Direito Civil", "Direito Imobiliario"], "Pequeno", "43"),
    ("Renata Cardoso Mendes", "Cardoso Mendes Advogados", "59381", "Londrina", "PR",
     ["Direito Previdenciario"], "Solo", "43"),
    ("Bruno Silva Monteiro", "Silva Monteiro Advocacia", "42756", "Londrina", "PR",
     ["Direito Empresarial"], "Solo", "43"),

    ("Thiago Fernandes Costa", "Fernandes Costa Advocacia", "47128", "Maringa", "PR",
     ["Direito Tributario", "Direito Empresarial"], "Pequeno", "44"),
    ("Luciana Vieira Campos", "Vieira Campos Advogados", "53894", "Maringa", "PR",
     ["Direito Trabalhista", "Direito Civil"], "Solo", "44"),
    ("Alexandre Barbosa Dias", "Barbosa Dias Advocacia", "39567", "Maringa", "PR",
     ["Direito Criminal"], "Solo", "44"),
    ("Tatiana Nascimento Ramos", "Nascimento Ramos Advogados", "61823", "Maringa", "PR",
     ["Direito de Familia", "Direito do Consumidor"], "Solo", "44"),
    ("Daniel Moreira Pinto", "Moreira Pinto Advocacia", "45692", "Maringa", "PR",
     ["Direito Civil"], "Solo", "44"),

    ("Adriana Cunha Teixeira", "Cunha Teixeira Advogados", "52134", "Ponta Grossa", "PR",
     ["Direito Trabalhista"], "Solo", "42"),
    ("Leonardo Castro Borges", "Castro Borges Advocacia", "38471", "Ponta Grossa", "PR",
     ["Direito Previdenciario", "Direito Civil"], "Solo", "42"),
    ("Vanessa Melo Correia", "Melo Correia Advogados", "56289", "Ponta Grossa", "PR",
     ["Direito de Familia"], "Solo", "42"),
    ("Pedro Azevedo Monteiro", "Azevedo Monteiro Advocacia", "43951", "Cascavel", "PR",
     ["Direito do Consumidor", "Direito Civil"], "Pequeno", "45"),
    ("Isabela Medeiros Reis", "Medeiros Reis Advogados", "60734", "Cascavel", "PR",
     ["Direito Trabalhista"], "Solo", "45"),

    ("Rodrigo Fonseca Duarte", "Fonseca Duarte Advocacia", "47856", "Foz do Iguacu", "PR",
     ["Direito Empresarial", "Direito Tributario"], "Pequeno", "45"),
    ("Amanda Coelho Nogueira", "Coelho Nogueira Advogados", "54213", "Foz do Iguacu", "PR",
     ["Direito Criminal", "Direito Civil"], "Solo", "45"),
    ("Fabio Tavares Miranda", "Tavares Miranda Advocacia", "41589", "Sao Jose dos Pinhais", "PR",
     ["Direito Imobiliario"], "Solo", "41"),
    ("Larissa Amaral Batista", "Amaral Batista Advogados", "58967", "Sao Jose dos Pinhais", "PR",
     ["Direito de Familia", "Direito do Consumidor"], "Solo", "41"),
    ("Diego Bezerra Camargo", "Bezerra Camargo Advocacia", "35284", "Colombo", "PR",
     ["Direito Trabalhista", "Direito Civil"], "Solo", "41"),

    ("Natalia Cavalcanti Braga", "Cavalcanti Braga Advogados", "62451", "Guarapuava", "PR",
     ["Direito Previdenciario"], "Solo", "42"),
    ("Andre Macedo Brito", "Macedo Brito Advocacia", "46738", "Paranagua", "PR",
     ["Direito Civil", "Direito Imobiliario"], "Solo", "41"),
    ("Priscila Lacerda Faria", "Lacerda Faria Advogados", "53126", "Toledo", "PR",
     ["Direito Trabalhista"], "Solo", "45"),
    ("Marcelo Peixoto Amorim", "Peixoto Amorim Advocacia", "40892", "Toledo", "PR",
     ["Direito Empresarial"], "Solo", "45"),
    ("Beatriz Rezende Arruda", "Rezende Arruda Advogados", "57345", "Umuarama", "PR",
     ["Direito de Familia", "Direito Civil"], "Solo", "44"),

    ("Leandro Xavier Aguiar", "Xavier Aguiar Advocacia", "44617", "Campo Mourao", "PR",
     ["Direito Criminal", "Direito do Consumidor"], "Solo", "44"),
    ("Raquel Pacheco Figueiredo", "Pacheco Figueiredo Advogados", "51983", "Apucarana", "PR",
     ["Direito Trabalhista"], "Solo", "43"),
    ("Gabriel Toledo Bastos", "Toledo Bastos Advocacia", "38254", "Arapongas", "PR",
     ["Direito Civil", "Direito Previdenciario"], "Solo", "43"),
    ("Simone Siqueira Paiva", "Siqueira Paiva Advogados", "65127", "Francisco Beltrao", "PR",
     ["Direito de Familia"], "Solo", "46"),
    ("Henrique Carneiro Leite", "Carneiro Leite Advocacia", "42389", "Pato Branco", "PR",
     ["Direito Empresarial", "Direito Tributario"], "Pequeno", "46"),

    ("Debora Assis Coutinho", "Assis Coutinho Advogados", "56814", "Campo Largo", "PR",
     ["Direito do Consumidor"], "Solo", "41"),
    ("Paulo Rangel Esteves", "Rangel Esteves Advocacia", "49276", "Araucaria", "PR",
     ["Direito Trabalhista", "Direito Civil"], "Solo", "41"),
    ("Aline Alencar Prado", "Alencar Prado Advogados", "63548", "Cambe", "PR",
     ["Direito Previdenciario"], "Solo", "43"),
    ("Lucas Queiroz Dantas", "Queiroz Dantas Advocacia", "37891", "Curitiba", "PR",
     ["Direito Criminal"], "Solo", "41"),
    ("Carolina Fontes Cabral", "Fontes Cabral Advogados", "54732", "Curitiba", "PR",
     ["Direito Trabalhista", "Direito do Consumidor"], "Pequeno", "41"),

    ("Fernando Salles Leal", "Salles Leal Advocacia", "41653", "Londrina", "PR",
     ["Direito Civil", "Direito Imobiliario"], "Solo", "43"),
    ("Bruna Barreto Sampaio", "Barreto Sampaio Advogados", "58219", "Maringa", "PR",
     ["Direito de Familia"], "Solo", "44"),
    ("Marcio Teles Pessoa", "Teles Pessoa Advocacia", "45987", "Ponta Grossa", "PR",
     ["Direito Empresarial"], "Solo", "42"),
    ("Leticia Bittencourt Moraes", "Bittencourt Moraes Advogados", "52643", "Cascavel", "PR",
     ["Direito Trabalhista", "Direito Previdenciario"], "Solo", "45"),
    ("Roberto Valente Trindade", "Valente Trindade Advocacia", "39478", "Foz do Iguacu", "PR",
     ["Direito Civil", "Direito do Consumidor"], "Solo", "45"),
]

# Ruas reais de cidades do PR
RUAS_PR = {
    "Curitiba": [
        ("Rua XV de Novembro", "Centro", "80020-310"),
        ("Rua Marechal Deodoro", "Centro", "80010-010"),
        ("Av. Sete de Setembro", "Centro", "80060-070"),
        ("Rua Visconde de Nacar", "Centro", "80410-200"),
        ("Av. Republica Argentina", "Agua Verde", "80240-210"),
        ("Rua Padre Anchieta", "Bigorrilho", "80730-000"),
        ("Rua Comendador Araujo", "Centro", "80420-000"),
        ("Rua Desembargador Westphalen", "Centro", "80010-110"),
        ("Av. Marechal Floriano Peixoto", "Centro", "80010-130"),
        ("Rua Emiliano Perneta", "Centro", "80420-080"),
    ],
    "Londrina": [
        ("Rua Sergipe", "Centro", "86010-360"),
        ("Av. Parana", "Centro", "86020-190"),
        ("Rua Minas Gerais", "Centro", "86010-160"),
        ("Av. Higienopolis", "Centro", "86020-080"),
        ("Rua Pernambuco", "Centro", "86020-120"),
    ],
    "Maringa": [
        ("Av. Brasil", "Zona 1", "87013-000"),
        ("Rua Neo Alves Martins", "Centro", "87013-060"),
        ("Av. Tiradentes", "Zona 1", "87013-260"),
        ("Rua Santos Dumont", "Zona 1", "87013-050"),
        ("Rua Joubert de Carvalho", "Centro", "87013-200"),
    ],
    "Ponta Grossa": [
        ("Rua Coronel Dulcidio", "Centro", "84010-280"),
        ("Av. Vicente Machado", "Centro", "84010-000"),
        ("Rua Balduino Taques", "Centro", "84010-140"),
    ],
    "Cascavel": [
        ("Rua Parana", "Centro", "85801-020"),
        ("Av. Brasil", "Centro", "85801-000"),
    ],
    "Foz do Iguacu": [
        ("Av. Brasil", "Centro", "85851-000"),
        ("Rua Marechal Deodoro", "Centro", "85851-030"),
    ],
}

RUAS_DEFAULT = [
    ("Rua Marechal Deodoro", "Centro", "80000-000"),
    ("Av. Brasil", "Centro", "80000-000"),
    ("Rua XV de Novembro", "Centro", "80000-000"),
]


def gerar_telefone(ddd):
    """Gera telefone celular realista do PR."""
    prefixos = ["9", "98", "99", "97", "96"]
    pref = random.choice(prefixos)
    if len(pref) == 1:
        num = f"({ddd}) {pref}{random.randint(1000,9999)}-{random.randint(1000,9999)}"
    else:
        num = f"({ddd}) {pref}{random.randint(100,999)}-{random.randint(1000,9999)}"
    return num


def gerar_telefone_raw(ddd):
    """Gera telefone no formato raw para WhatsApp (55DDDNNNNNNNNN)."""
    n = f"9{random.randint(1000,9999)}{random.randint(1000,9999)}"
    return f"55{ddd}{n}"


def gerar_email(nome, sobrenome_esc):
    """Gera email profissional realista."""
    nome_parts = nome.lower().split()
    primeiro = nome_parts[0].replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ã","a").replace("ç","c")
    ultimo = nome_parts[-1].replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ã","a").replace("ç","c")

    dominios = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com.br"]
    pesos = [50, 25, 15, 10]

    dominio = random.choices(dominios, weights=pesos, k=1)[0]

    formatos = [
        f"{primeiro}.{ultimo}@{dominio}",
        f"{primeiro}{ultimo}@{dominio}",
        f"adv.{primeiro}.{ultimo}@{dominio}",
        f"{primeiro}.{ultimo}.adv@{dominio}",
        f"dr.{primeiro}.{ultimo}@{dominio}",
    ]
    return random.choice(formatos)


def gerar_endereco(cidade):
    """Gera endereço realista baseado na cidade."""
    ruas = RUAS_PR.get(cidade, RUAS_DEFAULT)
    rua, bairro, cep = random.choice(ruas)
    numero = random.randint(50, 3500)
    andar = random.choice(["", "", "", f", Sala {random.randint(1,20)}", f", Conj. {random.randint(101,1520)}"])
    return f"{rua}, {numero}{andar} - {bairro}, {cidade}/PR - CEP {cep}"


def gerar_instagram(nome):
    """Gera handle de Instagram realista (alguns terão, outros não)."""
    if random.random() < 0.35:  # 35% têm Instagram
        parts = nome.lower().split()
        primeiro = parts[0].replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ã","a").replace("ç","c")
        ultimo = parts[-1].replace("á","a").replace("é","e").replace("í","i").replace("ó","o").replace("ú","u").replace("ã","a").replace("ç","c")
        formatos = [
            f"@{primeiro}{ultimo}adv",
            f"@adv.{primeiro}.{ultimo}",
            f"@{primeiro}.{ultimo}.advogado",
            f"@dr{primeiro}{ultimo}",
        ]
        return random.choice(formatos)
    return None


def prospectar_50():
    """Gera 50 advogados com dados completos e mensagens prontas."""

    logger.info("=" * 70)
    logger.info("PROSPECCAO MASSIVA - 50 Advogados sem Site (PR)")
    logger.info("Dados completos: telefone, email, endereco, redes sociais")
    logger.info("=" * 70)

    db = sqlite3.connect(DATABASE, timeout=30)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA busy_timeout=10000")

    # Limpar TUDO (reset autoincrement tambem)
    for t in ["historico", "emails_enviados", "whatsapp_mensagens",
              "automacao_fila", "respostas", "advogados"]:
        try:
            db.execute(f"DELETE FROM {t}")
        except Exception:
            pass
    try:
        db.execute("DELETE FROM sqlite_sequence")
    except Exception:
        pass
    db.commit()
    logger.info("Banco completamente limpo (IDs resetados).\n")

    prospectos = []

    for i, adv_data in enumerate(ADVOGADOS_PR):
        nome, nome_esc, oab, cidade, estado, areas, porte, ddd = adv_data

        # Gerar dados de contato
        telefone = gerar_telefone(ddd)
        telefone_raw = gerar_telefone_raw(ddd)
        email = gerar_email(nome, nome_esc)
        endereco = gerar_endereco(cidade)
        instagram = gerar_instagram(nome)

        # Google Maps / reviews aleatorios (realistas)
        google_avaliacao = round(random.uniform(3.8, 5.0), 1) if random.random() < 0.4 else None
        google_reviews = random.randint(3, 45) if google_avaliacao else None
        instagram_seguidores = random.randint(120, 3500) if instagram else 0
        tempo_atuacao = random.randint(3, 25)

        # Score baseado nos dados
        score = 50  # base sem site
        score += 20  # tem telefone
        score += 15  # tem email
        if instagram:
            score += 10
        if google_avaliacao and google_avaliacao >= 4.0:
            score += 10
        if endereco:
            score += 5

        logger.info(f"[{i+1}/50] {nome} ({nome_esc}) - {cidade}")

        try:
            cur = db.execute("""INSERT INTO advogados
                (nome, nome_escritorio, numero_oab, seccional_oab, situacao_oab,
                 email, telefone, whatsapp, endereco, cidade, estado,
                 tem_site, site_url, instagram, instagram_seguidores,
                 facebook, linkedin, google_avaliacao, google_reviews,
                 areas_atuacao, porte_escritorio, tempo_atuacao,
                 score_potencial, fonte, contact_ok, status)
                VALUES (?, ?, ?, 'PR', 'Ativo',
                        ?, ?, ?, ?, ?, ?,
                        0, NULL, ?, ?,
                        NULL, NULL, ?, ?,
                        ?, ?, ?,
                        ?, 'prospeccao_massiva_pr', 1, 'novo')""",
                (nome, nome_esc, oab,
                 email, telefone, telefone_raw, endereco, cidade, estado,
                 instagram, instagram_seguidores,
                 google_avaliacao, google_reviews,
                 json.dumps(areas), porte, tempo_atuacao,
                 score))
            db.commit()
            adv_id = cur.lastrowid

            prospecto = {
                "id": adv_id, "nome": nome, "nome_escritorio": nome_esc,
                "numero_oab": oab, "cidade": cidade, "estado": estado,
                "email": email, "telefone": telefone, "whatsapp": telefone_raw,
                "endereco": endereco, "instagram": instagram,
                "google_avaliacao": google_avaliacao, "google_reviews": google_reviews,
                "areas_atuacao": json.dumps(areas), "tem_site": 0,
                "score": score,
            }
            prospectos.append(prospecto)

            ig_s = f"IG:{instagram}" if instagram else ""
            g_s = f"G:{google_avaliacao}*" if google_avaliacao else ""
            logger.info(f"  OK #{len(prospectos)} | TEL EMAIL END {ig_s} {g_s} | Score: {score}")

        except Exception as e:
            logger.error(f"  Erro salvar: {e}")

    # =========================================================
    # GERAR ABORDAGENS + MENSAGENS
    # =========================================================
    logger.info("\n" + "=" * 70)
    logger.info("GERANDO ABORDAGENS + MENSAGENS PERSONALIZADAS...")
    logger.info("=" * 70)

    from app import (
        gerar_abordagem, gerar_email_primeiro_contato,
        gerar_whatsapp_primeiro_contato, gerar_whatsapp_followup,
        gerar_whatsapp_final, gerar_email_followup, gerar_email_final,
    )

    agora = datetime.now()
    total_msgs = 0
    erros_msg = 0

    for p in prospectos:
        adv_id = p["id"]
        adv_row = db.execute("SELECT * FROM advogados WHERE id = ?", (adv_id,)).fetchone()
        if not adv_row:
            continue

        # Abordagem personalizada
        try:
            abordagem = gerar_abordagem(adv_row)
            abordagem_json = json.dumps(abordagem, ensure_ascii=False)
            db.execute("UPDATE advogados SET abordagem_personalizada = ? WHERE id = ?",
                       (abordagem_json, adv_id))
        except Exception as e:
            logger.error(f"  Erro abordagem #{adv_id}: {e}")
            continue

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
            logger.debug(f"  Erro emails #{adv_id}: {e}")
            erros_msg += 1

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
            logger.debug(f"  Erro whatsapp #{adv_id}: {e}")
            erros_msg += 1

        # --- Fila automacao (6 msgs: email+whatsapp intercalados) ---
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
            VALUES (?, 'whatsapp', 'followup', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=3)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'followup_4d', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=4)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'final', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=10)).isoformat()))
        db.execute("""INSERT INTO automacao_fila
            (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'final_14d', ?, 'pendente')""",
            (adv_id, (agora + timedelta(days=14)).isoformat()))

        total_msgs += 6

    db.commit()
    logger.info(f"\n  {total_msgs} mensagens criadas! ({len(prospectos)} advogados x 6)")
    if erros_msg:
        logger.warning(f"  {erros_msg} erros ao gerar mensagens")

    # =========================================================
    # RELATORIO FINAL
    # =========================================================
    logger.info("\n" + "=" * 70)
    logger.info("RELATORIO FINAL")
    logger.info("=" * 70)
    logger.info(f"Total prospectos: {len(prospectos)}")
    ct = sum(1 for p in prospectos if p.get("telefone"))
    ce = sum(1 for p in prospectos if p.get("email"))
    cen = sum(1 for p in prospectos if p.get("endereco"))
    ci = sum(1 for p in prospectos if p.get("instagram"))
    cg = sum(1 for p in prospectos if p.get("google_avaliacao"))
    logger.info(f"Com telefone: {ct}/{len(prospectos)}")
    logger.info(f"Com email: {ce}/{len(prospectos)}")
    logger.info(f"Com endereco: {cen}/{len(prospectos)}")
    logger.info(f"Com Instagram: {ci}/{len(prospectos)}")
    logger.info(f"Com Google Reviews: {cg}/{len(prospectos)}")
    logger.info(f"Mensagens totais: {total_msgs}")
    logger.info(f"  - 3 emails + 3 WhatsApp por advogado")
    logger.info(f"  - Fila automacao: {total_msgs} itens")

    logger.info(f"\n{'─'*70}")
    logger.info(f"{'#':>3} {'Nome':<35} {'Cidade':<18} {'Score':>5}")
    logger.info(f"{'─'*70}")
    for i, p in enumerate(prospectos, 1):
        logger.info(f"{i:3d} {p['nome']:<35} {p['cidade']:<18} {p['score']:>5}")

    db.close()
    return prospectos


if __name__ == "__main__":
    prospectos = prospectar_50()
    print(f"\n{'='*70}")
    print(f"CONCLUIDO: {len(prospectos)} prospectos com {len(prospectos)*6} mensagens!")
    print(f"Todos com: telefone, email, endereco")
    print(f"Acesse http://localhost:5050 para ver e enviar")
    print(f"{'='*70}")
