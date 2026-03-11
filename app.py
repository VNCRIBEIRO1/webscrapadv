"""
ProspectAdv — Plataforma de Prospecção de Advogados/Escritórios sem Site
Flask Monolith com SQLite, Gmail OAuth, WhatsApp automatizado
"""

import os
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from functools import wraps

from flask import (
    Flask, render_template, request, jsonify, redirect,
    url_for, session, flash, g
)
from dotenv import load_dotenv

load_dotenv()

# ─── Configuração ──────────────────────────────────────────
app = Flask(__name__)
app.secret_key = os.getenv("FLASK_SECRET_KEY", "prospectadv-dev-secret-2026")
app.config["DATABASE"] = "prospeccao_adv.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ProspectAdv")

# ─── Identidade do Vendedor ────────────────────────────────
VENDEDOR = {
    "nome": "Vinicius Ribeiro dos Anjos",
    "instagram": "@vranjos",
    "telefone": "(18) 99631-1933",
    "whatsapp": "5518996311933",
    "whatsapp_link": "https://wa.me/5518996311933",
    "portfolio": "https://cerbeleraeoliveiraadv.vercel.app/",
    "email_profissional": "contato@vranjos.com",
}

# ─── Limites diários ───────────────────────────────────────
LIMITE_DIARIO_EMAILS = 50
LIMITE_DIARIO_WHATSAPP = 100

# ─── Argumentos de venda por área ──────────────────────────
ARGUMENTOS_POR_AREA = {
    "Direito Trabalhista": (
        "Trabalhadores buscam advogados no Google quando são demitidos. "
        "Sem site, o(a) Sr(a). perde clientes que estão prontos para contratar AGORA. "
        "Um site com calculadora de rescisão atrai leads qualificados 24h/dia."
    ),
    "Direito Criminal": (
        "Em situações de urgência criminal, a família busca 'advogado criminalista [cidade]' no Google. "
        "Sem site, o(a) Sr(a). não aparece nessa busca — e o cliente vai para o concorrente."
    ),
    "Direito Civil": (
        "Ações cíveis são amplamente buscadas no Google. Um site com blog sobre direitos do cidadão "
        "gera tráfego orgânico e posiciona o(a) Sr(a). como referência."
    ),
    "Direito do Consumidor": (
        "Ações de consumidor são as mais buscadas no Google. Um site com blog sobre "
        "'direitos do consumidor' gera tráfego orgânico e posiciona o(a) Sr(a). como referência."
    ),
    "Direito Empresarial": (
        "Empresários pesquisam online antes de contratar assessoria jurídica. "
        "Um site institucional profissional transmite a credibilidade que o cliente corporativo espera."
    ),
    "Direito de Família": (
        "Pessoas passando por divórcio buscam 'advogado divórcio [cidade]' no Google. "
        "Um site com agendamento online facilita o primeiro contato nesse momento delicado."
    ),
    "Direito Previdenciário": (
        "Aposentadoria e benefícios do INSS são temas amplamente buscados. "
        "Um site com calculadora de tempo de contribuição atrai dezenas de consultas mensais."
    ),
}

# ─── Banco de Dados ────────────────────────────────────────
def get_db():
    """Retorna conexão SQLite com Row factory."""
    if "db" not in g:
        g.db = sqlite3.connect(app.config["DATABASE"])
        g.db.row_factory = sqlite3.Row
        g.db.execute("PRAGMA journal_mode=WAL")
        g.db.execute("PRAGMA foreign_keys=ON")
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def init_db():
    """Cria todas as tabelas se não existirem."""
    db = sqlite3.connect(app.config["DATABASE"])
    db.executescript("""
        CREATE TABLE IF NOT EXISTS advogados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            nome TEXT NOT NULL,
            nome_escritorio TEXT,
            numero_oab TEXT,
            seccional_oab TEXT,
            situacao_oab TEXT,
            tipo_inscricao TEXT,
            email TEXT,
            telefone TEXT,
            whatsapp TEXT,
            endereco TEXT,
            cidade TEXT,
            estado TEXT,
            cep TEXT,
            tem_site INTEGER DEFAULT 0,
            site_url TEXT,
            instagram TEXT,
            instagram_seguidores INTEGER DEFAULT 0,
            facebook TEXT,
            facebook_seguidores INTEGER DEFAULT 0,
            linkedin TEXT,
            google_maps_url TEXT,
            google_avaliacao REAL,
            google_reviews INTEGER,
            areas_atuacao TEXT,
            especialidades TEXT,
            tempo_atuacao INTEGER,
            volume_processos INTEGER,
            tipo_clientela TEXT,
            porte_escritorio TEXT,
            score_potencial INTEGER DEFAULT 0,
            motivo_abordagem TEXT,
            status TEXT DEFAULT 'novo',
            abordagem_personalizada TEXT,
            fonte TEXT,
            foto TEXT,
            notas TEXT,
            data_criacao TEXT DEFAULT (datetime('now')),
            data_contato TEXT,
            proxima_acao TEXT,
            data_proxima_acao TEXT
        );

        CREATE TABLE IF NOT EXISTS emails_enviados (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advogado_id INTEGER,
            tipo TEXT,
            assunto TEXT,
            corpo_html TEXT,
            corpo_texto TEXT,
            data_envio TEXT,
            message_id TEXT,
            thread_id TEXT,
            status TEXT DEFAULT 'enviado',
            conta_gmail TEXT,
            FOREIGN KEY (advogado_id) REFERENCES advogados(id)
        );

        CREATE TABLE IF NOT EXISTS whatsapp_mensagens (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advogado_id INTEGER,
            tipo TEXT,
            mensagem TEXT,
            data_envio TEXT,
            status TEXT DEFAULT 'pendente',
            resposta TEXT,
            data_resposta TEXT,
            FOREIGN KEY (advogado_id) REFERENCES advogados(id)
        );

        CREATE TABLE IF NOT EXISTS automacao_fila (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advogado_id INTEGER,
            canal TEXT,
            tipo_mensagem TEXT,
            data_agendada TEXT,
            status TEXT DEFAULT 'pendente',
            conta TEXT,
            FOREIGN KEY (advogado_id) REFERENCES advogados(id)
        );

        CREATE TABLE IF NOT EXISTS respostas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advogado_id INTEGER,
            canal TEXT,
            conteudo TEXT,
            data_recebimento TEXT,
            sentimento TEXT,
            FOREIGN KEY (advogado_id) REFERENCES advogados(id)
        );

        CREATE TABLE IF NOT EXISTS historico (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            advogado_id INTEGER,
            acao TEXT,
            detalhes TEXT,
            data TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (advogado_id) REFERENCES advogados(id)
        );

        CREATE TABLE IF NOT EXISTS gmail_contas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE,
            nome_exibicao TEXT,
            ativo INTEGER DEFAULT 1,
            token_path TEXT,
            envios_hoje INTEGER DEFAULT 0,
            ultimo_reset TEXT
        );
    """)
    db.commit()
    db.close()
    logger.info("Banco de dados inicializado com sucesso.")


# ─── Helpers ───────────────────────────────────────────────
def _url_valida(url):
    """Valida se uma URL de rede social é válida."""
    if not url:
        return False
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        return False
    dominios_validos = [
        "instagram.com", "facebook.com", "linkedin.com",
        "twitter.com", "x.com", "wa.me", "google.com",
    ]
    return any(d in url.lower() for d in dominios_validos)


def calcular_score(adv):
    """Calcula score de potencial (0-100) para o advogado."""
    score = 0

    # SEM SITE = principal critério (+30)
    tem_site = adv["tem_site"] if isinstance(adv, dict) else adv[14]
    if not tem_site:
        score += 30

    # Presença digital parcial (+20)
    redes = 0
    instagram = adv.get("instagram") if isinstance(adv, dict) else adv[16]
    facebook = adv.get("facebook") if isinstance(adv, dict) else adv[18]
    linkedin = adv.get("linkedin") if isinstance(adv, dict) else adv[20]
    if instagram:
        redes += 1
    if facebook:
        redes += 1
    if linkedin:
        redes += 1
    if redes >= 1 and not tem_site:
        score += min(redes * 7, 20)

    # Google Maps com reviews (+15)
    google_avaliacao = adv.get("google_avaliacao") if isinstance(adv, dict) else adv[22]
    google_reviews = adv.get("google_reviews") if isinstance(adv, dict) else adv[23]
    if google_avaliacao and google_avaliacao >= 4.0:
        score += 10
    if google_reviews and google_reviews >= 5:
        score += 5

    # Tempo de atuação (+10)
    tempo = adv.get("tempo_atuacao") if isinstance(adv, dict) else adv[26]
    if tempo:
        if tempo >= 5:
            score += 10
        elif tempo >= 2:
            score += 5

    # Volume de processos (+10)
    volume = adv.get("volume_processos") if isinstance(adv, dict) else adv[27]
    if volume and volume >= 10:
        score += 10
    elif volume and volume >= 5:
        score += 5

    # Porte do escritório (+10)
    porte = adv.get("porte_escritorio") if isinstance(adv, dict) else adv[29]
    if porte in ("Pequeno", "Médio", "Medio"):
        score += 10
    elif porte == "Solo":
        score += 5

    # Múltiplas áreas (+5)
    areas_raw = adv.get("areas_atuacao") if isinstance(adv, dict) else adv[24]
    try:
        areas = json.loads(areas_raw or "[]")
    except (json.JSONDecodeError, TypeError):
        areas = []
    if len(areas) >= 3:
        score += 5

    return min(score, 100)


def registrar_historico(advogado_id, acao, detalhes=""):
    """Registra ação no histórico."""
    db = get_db()
    db.execute(
        "INSERT INTO historico (advogado_id, acao, detalhes) VALUES (?, ?, ?)",
        (advogado_id, acao, detalhes),
    )
    db.commit()


def contar_emails_hoje(conta=None):
    """Conta emails enviados hoje por conta."""
    db = get_db()
    hoje = datetime.now().strftime("%Y-%m-%d")
    if conta:
        row = db.execute(
            "SELECT COUNT(*) FROM emails_enviados WHERE data_envio LIKE ? AND conta_gmail = ?",
            (f"{hoje}%", conta),
        ).fetchone()
    else:
        row = db.execute(
            "SELECT COUNT(*) FROM emails_enviados WHERE data_envio LIKE ?",
            (f"{hoje}%",),
        ).fetchone()
    return row[0] if row else 0


def contar_whatsapp_hoje():
    """Conta mensagens WhatsApp enviadas hoje."""
    db = get_db()
    hoje = datetime.now().strftime("%Y-%m-%d")
    row = db.execute(
        "SELECT COUNT(*) FROM whatsapp_mensagens WHERE data_envio LIKE ?",
        (f"{hoje}%",),
    ).fetchone()
    return row[0] if row else 0


# ─── Geração de abordagem personalizada ────────────────────
def gerar_abordagem(adv):
    """Gera JSON de abordagem personalizada para o advogado."""
    areas = json.loads(adv["areas_atuacao"] or "[]") if adv["areas_atuacao"] else []
    area_principal = areas[0] if areas else "Direito"

    argumento_area = ARGUMENTOS_POR_AREA.get(
        area_principal,
        "Um site institucional profissional amplia sua presença digital e atrai novos clientes."
    )

    necessidades = []
    if not adv["tem_site"]:
        necessidades.append(
            "Sem site institucional — potenciais clientes não encontram informações online"
        )
    if adv["instagram"] and not adv["tem_site"]:
        necessidades.append(
            f"Presença no Instagram ({adv['instagram']}) mas sem landing page para converter seguidores"
        )
    if adv["google_avaliacao"] and adv["google_avaliacao"] >= 4.0:
        necessidades.append(
            f"Avaliações positivas no Google ({adv['google_avaliacao']}⭐) mas sem site para captar tráfego orgânico"
        )

    abordagem = {
        "versao": "v1",
        "data_enriquecimento": datetime.now().isoformat(),
        "perfil": {
            "nome": adv["nome"],
            "escritorio": adv["nome_escritorio"] or adv["nome"],
            "oab": f"OAB/{adv['seccional_oab']} {adv['numero_oab']}" if adv["numero_oab"] else "",
            "cidade": adv["cidade"] or "",
            "estado": adv["estado"] or "",
            "tempo_atuacao_anos": adv["tempo_atuacao"] or 0,
            "porte": adv["porte_escritorio"] or "Solo",
        },
        "areas_atuacao": areas,
        "area_principal": area_principal,
        "presenca_digital": {
            "tem_site": bool(adv["tem_site"]),
            "instagram": adv["instagram"] or "",
            "instagram_seguidores": adv["instagram_seguidores"] or 0,
            "facebook": adv["facebook"] or "",
            "facebook_seguidores": adv["facebook_seguidores"] or 0,
            "linkedin": adv["linkedin"] or "",
            "google_avaliacao": adv["google_avaliacao"],
            "google_reviews": adv["google_reviews"] or 0,
        },
        "argumentos": {
            "abertura_personalizada": f"Notei que o(a) Sr(a). tem uma atuação forte em {area_principal} em {adv['cidade'] or 'sua cidade'}",
            "argumento_principal": "Um site institucional profissional é o cartão de visitas digital do seu escritório.",
            "argumento_area": argumento_area,
            "argumento_digital": (
                f"Com {adv['google_reviews'] or 0} avaliações no Google e nota {adv['google_avaliacao'] or 'N/A'}⭐, "
                "seu escritório já tem credibilidade — falta apenas o site para consolidar."
            ),
            "argumento_competitivo": "Seus concorrentes com site captam os clientes que buscam no Google.",
            "argumento_etico": "Site com caráter informativo, em conformidade com o Provimento 205/2021 da OAB.",
        },
        "tom": "profissional",
        "necessidades_identificadas": necessidades,
        "oferta_personalizada": {
            "site_modelo": "institucional_completo",
            "features_sugeridas": [
                "Agendamento online", "Chatbot triagem",
                "Blog jurídico", "Calculadora de direitos"
            ],
            "referencia_portfolio": VENDEDOR["portfolio"],
            "investimento_sugerido": "a_partir_de_2500",
        },
    }
    return abordagem


# ─── Templates de Email ────────────────────────────────────
def gerar_email_primeiro_contato(adv, abordagem):
    """Gera o email de primeiro contato (Dia 0)."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]
    area = ab["area_principal"]
    cidade = ab["perfil"]["cidade"]
    reviews = ab["presenca_digital"]["google_reviews"]
    avaliacao = ab["presenca_digital"]["google_avaliacao"]
    instagram = ab["presenca_digital"]["instagram"]
    seguidores = ab["presenca_digital"]["instagram_seguidores"]

    assunto = f"Dr(a). {nome.split()[0]} — {area}: seus clientes estão procurando você online"

    mencao_google = ""
    if reviews and reviews > 0:
        mencao_google = (
            f"<p>Notei que seu escritório possui <strong>{reviews} avaliações no Google</strong> "
            f"com nota <strong>{avaliacao}⭐</strong> — uma excelente reputação! "
            "Mas sem um site, esse potencial não se converte em novos clientes.</p>"
        )

    mencao_instagram = ""
    if instagram:
        mencao_instagram = (
            f"<p>Vi também que o(a) Sr(a). mantém o perfil <strong>{instagram}</strong> "
            f"no Instagram com <strong>{seguidores} seguidores</strong>. Um site seria o destino "
            "perfeito para converter esse público em consultas agendadas.</p>"
        )

    corpo_html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #333;">
        <p>Prezado(a) Dr(a). <strong>{nome}</strong>,</p>

        <p>Me chamo {VENDEDOR['nome']}, sou web designer especializado em
        <strong>sites para escritórios de advocacia</strong>.</p>

        <p>Notei que o(a) Sr(a). tem uma atuação forte em <strong>{area}</strong>
        em <strong>{cidade}</strong>, mas ainda <strong>não possui um site institucional</strong>.</p>

        {mencao_google}
        {mencao_instagram}

        <p><strong>Sabia que 87% das pessoas pesquisam um advogado no Google antes de ligar?</strong>
        Sem site, esses potenciais clientes vão para a concorrência.</p>

        <p>Desenvolvi recentemente um site para o escritório <strong>Cerbelera & Oliveira Advogados</strong>, com:</p>
        <ul>
            <li>✅ Design profissional e ético (Provimento 205/2021 OAB)</li>
            <li>✅ Chatbot de triagem para atendimento 24h</li>
            <li>✅ Agendamento online de consultas</li>
            <li>✅ Blog jurídico para captação orgânica</li>
            <li>✅ Calculadora de direitos trabalhistas</li>
        </ul>

        <p>🔗 <a href="{VENDEDOR['portfolio']}" style="color: #1a5276;">
        Veja o portfólio aqui</a></p>

        <p>Posso preparar uma <strong>proposta personalizada sem compromisso</strong>
        para o seu escritório. Que tal uma conversa rápida de 15 minutos?</p>

        <p>Aguardo seu retorno!</p>

        <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
        <p style="font-size: 13px; color: #666;">
            <strong>{VENDEDOR['nome']}</strong><br>
            Web Designer — Sites para Advocacia<br>
            📱 {VENDEDOR['telefone']}<br>
            💬 <a href="{VENDEDOR['whatsapp_link']}">WhatsApp</a><br>
            🌐 <a href="{VENDEDOR['portfolio']}">{VENDEDOR['portfolio']}</a>
        </p>

        <p style="font-size: 11px; color: #999;">
            <em>Site com caráter meramente informativo, em conformidade com o
            Provimento 205/2021 do Conselho Federal da OAB.</em>
        </p>
    </div>
    """

    corpo_texto = (
        f"Prezado(a) Dr(a). {nome},\n\n"
        f"Me chamo {VENDEDOR['nome']}, sou web designer especializado em sites para escritórios de advocacia.\n\n"
        f"Notei que o(a) Sr(a). tem uma atuação forte em {area} em {cidade}, "
        "mas ainda não possui um site institucional.\n\n"
        "87% das pessoas pesquisam um advogado no Google antes de ligar. "
        "Sem site, esses potenciais clientes vão para a concorrência.\n\n"
        f"Veja meu portfólio: {VENDEDOR['portfolio']}\n\n"
        "Posso preparar uma proposta personalizada sem compromisso.\n\n"
        f"{VENDEDOR['nome']}\n{VENDEDOR['telefone']}\n{VENDEDOR['whatsapp_link']}"
    )

    return assunto, corpo_html, corpo_texto


def gerar_email_followup(adv, abordagem):
    """Gera o email de follow-up (Dia 4)."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]
    area = ab["area_principal"]

    assunto = f"Re: Dado interessante sobre {area} e presença digital"

    corpo_html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #333;">
        <p>Dr(a). <strong>{nome.split()[0]}</strong>,</p>

        <p>Enviei uma mensagem há alguns dias sobre a criação de um site para seu escritório.
        Gostaria de compartilhar um dado relevante:</p>

        <blockquote style="border-left: 3px solid #1a5276; padding-left: 15px; margin: 15px 0; color: #555;">
            <strong>87% dos brasileiros pesquisam um profissional no Google antes de contratar.</strong>
            Para advogados, esse número chega a <strong>92%</strong> em áreas como {area}.
        </blockquote>

        <p>O escritório <strong>Cerbelera & Oliveira Advogados</strong> implementou um site com
        chatbot de triagem e viu um <strong>aumento significativo</strong> nas consultas online.
        O site deles tem nota <strong>4.9⭐ no Google</strong>.</p>

        <p>É importante destacar que o <strong>Provimento 205/2021 da OAB</strong> não apenas
        <em>permite</em>, mas <strong>incentiva</strong> a presença digital dos advogados,
        desde que com caráter informativo.</p>

        <p>Gostaria de agendar uma conversa rápida para mostrar como ficaria um site
        personalizado para o(a) Sr(a).?</p>

        <p>Pode responder este email ou me chamar no WhatsApp:</p>
        <p>📱 <a href="{VENDEDOR['whatsapp_link']}">{VENDEDOR['telefone']}</a></p>

        <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
        <p style="font-size: 13px; color: #666;">
            <strong>{VENDEDOR['nome']}</strong><br>
            📱 {VENDEDOR['telefone']} | 🌐 <a href="{VENDEDOR['portfolio']}">{VENDEDOR['portfolio']}</a>
        </p>
    </div>
    """

    corpo_texto = (
        f"Dr(a). {nome.split()[0]},\n\n"
        "Enviei uma mensagem há alguns dias sobre a criação de um site para seu escritório.\n\n"
        f"87% dos brasileiros pesquisam um profissional no Google antes de contratar. "
        f"Para advogados, esse número chega a 92% em áreas como {area}.\n\n"
        "O Provimento 205/2021 da OAB não apenas permite, mas incentiva a presença digital.\n\n"
        f"Responda este email ou WhatsApp: {VENDEDOR['telefone']}\n\n"
        f"{VENDEDOR['nome']}"
    )

    return assunto, corpo_html, corpo_texto


def gerar_email_final(adv, abordagem):
    """Gera o email final (Dia 14)."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]
    escritorio = ab["perfil"]["escritorio"]

    assunto = f"Última mensagem — oportunidade para {escritorio}"

    corpo_html = f"""
    <div style="font-family: 'Segoe UI', Arial, sans-serif; max-width: 600px; margin: 0 auto; color: #333;">
        <p>Dr(a). <strong>{nome.split()[0]}</strong>,</p>

        <p>Esta é minha última mensagem sobre a criação de um site institucional
        para o <strong>{escritorio}</strong>.</p>

        <p>Respeito muito o seu tempo e não gostaria de ser inconveniente. Apenas gostaria
        de reforçar os benefícios que um site profissional traz:</p>

        <ul>
            <li>📍 Ser encontrado no Google por potenciais clientes</li>
            <li>🤖 Chatbot de triagem atendendo 24h/dia</li>
            <li>📅 Agendamento online que facilita o primeiro contato</li>
            <li>📝 Blog jurídico que posiciona como referência na área</li>
            <li>⚖️ Tudo em conformidade com o Provimento 205/2021 OAB</li>
        </ul>

        <p>🔗 <a href="{VENDEDOR['portfolio']}">Veja um exemplo real</a></p>

        <p>Caso tenha interesse no futuro, estarei à disposição.</p>

        <p>Desejo muito sucesso ao(à) Sr(a). e ao escritório! 🙏</p>

        <hr style="border: none; border-top: 1px solid #ddd; margin: 20px 0;">
        <p style="font-size: 13px; color: #666;">
            <strong>{VENDEDOR['nome']}</strong><br>
            📱 {VENDEDOR['telefone']} | 🌐 <a href="{VENDEDOR['portfolio']}">{VENDEDOR['portfolio']}</a>
        </p>
    </div>
    """

    corpo_texto = (
        f"Dr(a). {nome.split()[0]},\n\n"
        f"Esta é minha última mensagem sobre a criação de um site para o {escritorio}.\n\n"
        "Caso tenha interesse no futuro, estarei à disposição.\n\n"
        f"Portfólio: {VENDEDOR['portfolio']}\n\n"
        f"Desejo sucesso! 🙏\n\n{VENDEDOR['nome']}\n{VENDEDOR['telefone']}"
    )

    return assunto, corpo_html, corpo_texto


# ─── Templates de WhatsApp ─────────────────────────────────
def gerar_whatsapp_primeiro_contato(adv, abordagem):
    """Gera mensagem WhatsApp de primeiro contato."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]
    area = ab["area_principal"]
    cidade = ab["perfil"]["cidade"]
    reviews = ab["presenca_digital"]["google_reviews"]
    avaliacao = ab["presenca_digital"]["google_avaliacao"]

    reviews_txt = ""
    if reviews and reviews > 0:
        reviews_txt = f", com {reviews} avaliações positivas no Google ({avaliacao}⭐)"

    msg = (
        f"Olá Dr(a). {nome.split()[0]}, tudo bem? 👋\n\n"
        f"Me chamo {VENDEDOR['nome']}, sou web designer especializado em sites para escritórios de advocacia.\n\n"
        f"Notei que o(a) Sr(a). tem uma atuação forte em {area} em {cidade}"
        f"{reviews_txt}, mas ainda não possui um site institucional.\n\n"
        "Desenvolvi recentemente um site para o escritório Cerbelera & Oliveira Advogados, com:\n"
        "✅ Design profissional e ético (Provimento 205/2021)\n"
        "✅ Chatbot de triagem para atendimento 24h\n"
        "✅ Agendamento online de consultas\n"
        "✅ Blog jurídico para captação orgânica\n\n"
        "Posso mostrar em 5 minutos como ficaria para o seu escritório?\n\n"
        f"🔗 Veja o portfólio: {VENDEDOR['portfolio']}\n\n"
        f"Abraço,\n{VENDEDOR['nome']}\n📱 {VENDEDOR['telefone']}"
    )
    return msg


def gerar_whatsapp_followup(adv, abordagem):
    """Gera mensagem WhatsApp de follow-up (Dia 3)."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]
    instagram = ab["presenca_digital"]["instagram"]
    seguidores = ab["presenca_digital"]["instagram_seguidores"]

    mencao_ig = ""
    if instagram:
        mencao_ig = (
            f"\nVi que o(a) Sr(a). tem um perfil ativo no Instagram ({instagram}) "
            f"com {seguidores} seguidores. Um site seria o destino perfeito para "
            "converter esse público em consultas agendadas.\n"
        )

    msg = (
        f"Dr(a). {nome.split()[0]}, boa tarde!\n\n"
        "Enviei uma mensagem há alguns dias sobre a criação de um site para seu escritório.\n\n"
        "Um dado interessante: 87% das pessoas pesquisam um advogado no Google antes de ligar. "
        "Sem site, esses potenciais clientes vão para a concorrência.\n"
        f"{mencao_ig}\n"
        "Posso preparar uma proposta personalizada sem compromisso?\n\n"
        f"{VENDEDOR['nome']}\n📱 {VENDEDOR['telefone']}"
    )
    return msg


def gerar_whatsapp_final(adv, abordagem):
    """Gera mensagem WhatsApp final (Dia 10)."""
    ab = abordagem if isinstance(abordagem, dict) else json.loads(abordagem)
    nome = ab["perfil"]["nome"]

    msg = (
        f"Dr(a). {nome.split()[0]}, última mensagem sobre o site institucional.\n\n"
        "Caso tenha interesse no futuro, fico à disposição. "
        f"Meu trabalho pode ser conferido em: {VENDEDOR['portfolio']}\n\n"
        f"Desejo sucesso no seu escritório! 🙏\n\n"
        f"{VENDEDOR['nome']} | {VENDEDOR['telefone']}"
    )
    return msg


# ─── ROTAS — Páginas HTML ──────────────────────────────────

@app.route("/")
def dashboard():
    """Painel principal com estatísticas."""
    db = get_db()

    total = db.execute("SELECT COUNT(*) FROM advogados").fetchone()[0]
    sem_site = db.execute("SELECT COUNT(*) FROM advogados WHERE tem_site = 0").fetchone()[0]
    novos = db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'novo'").fetchone()[0]
    contatados = db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'contatado'").fetchone()[0]
    interessados = db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'interessado'").fetchone()[0]
    fechados = db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'fechado'").fetchone()[0]
    perdidos = db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'perdido'").fetchone()[0]

    emails_hoje = contar_emails_hoje()
    whatsapp_hoje = contar_whatsapp_hoje()

    total_emails = db.execute("SELECT COUNT(*) FROM emails_enviados").fetchone()[0]
    total_whatsapp = db.execute("SELECT COUNT(*) FROM whatsapp_mensagens WHERE status != 'pendente'").fetchone()[0]
    total_respostas = db.execute("SELECT COUNT(*) FROM respostas").fetchone()[0]

    # Score médio
    row_score = db.execute("SELECT AVG(score_potencial) FROM advogados WHERE score_potencial > 0").fetchone()
    score_medio = round(row_score[0], 1) if row_score[0] else 0

    # Top estados
    estados = db.execute(
        "SELECT estado, COUNT(*) as cnt FROM advogados WHERE estado IS NOT NULL GROUP BY estado ORDER BY cnt DESC LIMIT 10"
    ).fetchall()

    # Top áreas
    top_areas = db.execute(
        "SELECT areas_atuacao FROM advogados WHERE areas_atuacao IS NOT NULL"
    ).fetchall()
    area_count = {}
    for row in top_areas:
        try:
            for area in json.loads(row[0] or "[]"):
                area_count[area] = area_count.get(area, 0) + 1
        except (json.JSONDecodeError, TypeError):
            pass
    top_areas_sorted = sorted(area_count.items(), key=lambda x: x[1], reverse=True)[:8]

    # Fila pendente
    fila_pendente = db.execute("SELECT COUNT(*) FROM automacao_fila WHERE status = 'pendente'").fetchone()[0]

    stats = {
        "total": total,
        "sem_site": sem_site,
        "novos": novos,
        "contatados": contatados,
        "interessados": interessados,
        "fechados": fechados,
        "perdidos": perdidos,
        "emails_hoje": emails_hoje,
        "whatsapp_hoje": whatsapp_hoje,
        "limite_emails": LIMITE_DIARIO_EMAILS,
        "limite_whatsapp": LIMITE_DIARIO_WHATSAPP,
        "total_emails": total_emails,
        "total_whatsapp": total_whatsapp,
        "total_respostas": total_respostas,
        "score_medio": score_medio,
        "estados": estados,
        "top_areas": top_areas_sorted,
        "fila_pendente": fila_pendente,
    }

    return render_template("dashboard.html", stats=stats, vendedor=VENDEDOR)


@app.route("/advogados")
def listar_advogados():
    """Lista de leads com filtros."""
    db = get_db()

    filtro_status = request.args.get("status", "")
    filtro_estado = request.args.get("estado", "")
    filtro_area = request.args.get("area", "")
    filtro_score_min = request.args.get("score_min", "")
    busca = request.args.get("q", "")
    pagina = int(request.args.get("p", 1))
    por_pagina = 50

    query = "SELECT * FROM advogados WHERE 1=1"
    params = []

    if filtro_status:
        query += " AND status = ?"
        params.append(filtro_status)
    if filtro_estado:
        query += " AND estado = ?"
        params.append(filtro_estado)
    if filtro_area:
        query += " AND areas_atuacao LIKE ?"
        params.append(f"%{filtro_area}%")
    if filtro_score_min:
        query += " AND score_potencial >= ?"
        params.append(int(filtro_score_min))
    if busca:
        query += " AND (nome LIKE ? OR nome_escritorio LIKE ? OR email LIKE ? OR cidade LIKE ?)"
        params.extend([f"%{busca}%"] * 4)

    # Total para paginação
    count_query = query.replace("SELECT *", "SELECT COUNT(*)", 1)
    total = db.execute(count_query, params).fetchone()[0]

    query += " ORDER BY score_potencial DESC, data_criacao DESC LIMIT ? OFFSET ?"
    params.extend([por_pagina, (pagina - 1) * por_pagina])

    advogados = db.execute(query, params).fetchall()
    total_paginas = max(1, (total + por_pagina - 1) // por_pagina)

    # Listas para filtros
    estados = db.execute(
        "SELECT DISTINCT estado FROM advogados WHERE estado IS NOT NULL ORDER BY estado"
    ).fetchall()

    return render_template(
        "advogados.html",
        advogados=advogados,
        pagina=pagina,
        total_paginas=total_paginas,
        total=total,
        filtro_status=filtro_status,
        filtro_estado=filtro_estado,
        filtro_area=filtro_area,
        filtro_score_min=filtro_score_min,
        busca=busca,
        estados=estados,
        vendedor=VENDEDOR,
    )


@app.route("/advogado/<int:id>")
def detalhe_advogado(id):
    """Detalhe completo do advogado."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        flash("Advogado não encontrado.", "erro")
        return redirect(url_for("listar_advogados"))

    emails = db.execute(
        "SELECT * FROM emails_enviados WHERE advogado_id = ? ORDER BY data_envio DESC", (id,)
    ).fetchall()

    whatsapp = db.execute(
        "SELECT * FROM whatsapp_mensagens WHERE advogado_id = ? ORDER BY data_envio DESC", (id,)
    ).fetchall()

    resps = db.execute(
        "SELECT * FROM respostas WHERE advogado_id = ? ORDER BY data_recebimento DESC", (id,)
    ).fetchall()

    hist = db.execute(
        "SELECT * FROM historico WHERE advogado_id = ? ORDER BY data DESC", (id,)
    ).fetchall()

    abordagem = None
    if adv["abordagem_personalizada"]:
        try:
            abordagem = json.loads(adv["abordagem_personalizada"])
        except (json.JSONDecodeError, TypeError):
            pass

    areas = []
    if adv["areas_atuacao"]:
        try:
            areas = json.loads(adv["areas_atuacao"])
        except (json.JSONDecodeError, TypeError):
            pass

    return render_template(
        "detalhe.html",
        adv=adv,
        emails=emails,
        whatsapp=whatsapp,
        respostas=resps,
        historico=hist,
        abordagem=abordagem,
        areas=areas,
        vendedor=VENDEDOR,
    )


@app.route("/automacao")
def automacao():
    """Painel de automação de envios."""
    db = get_db()

    fila = db.execute("""
        SELECT af.*, a.nome, a.nome_escritorio, a.cidade, a.estado
        FROM automacao_fila af
        JOIN advogados a ON af.advogado_id = a.id
        ORDER BY af.data_agendada ASC
        LIMIT 200
    """).fetchall()

    contas = db.execute("SELECT * FROM gmail_contas ORDER BY email").fetchall()

    return render_template(
        "automacao.html",
        fila=fila,
        contas=contas,
        emails_hoje=contar_emails_hoje(),
        whatsapp_hoje=contar_whatsapp_hoje(),
        limite_emails=LIMITE_DIARIO_EMAILS,
        limite_whatsapp=LIMITE_DIARIO_WHATSAPP,
        vendedor=VENDEDOR,
    )


@app.route("/respostas")
def respostas():
    """Respostas recebidas."""
    db = get_db()

    resps = db.execute("""
        SELECT r.*, a.nome, a.nome_escritorio, a.cidade, a.estado, a.score_potencial
        FROM respostas r
        JOIN advogados a ON r.advogado_id = a.id
        ORDER BY r.data_recebimento DESC
        LIMIT 200
    """).fetchall()

    return render_template("respostas.html", respostas=resps, vendedor=VENDEDOR)


@app.route("/whatsapp")
def whatsapp_painel():
    """Painel de WhatsApp."""
    db = get_db()

    mensagens = db.execute("""
        SELECT wm.*, a.nome, a.nome_escritorio, a.cidade, a.whatsapp
        FROM whatsapp_mensagens wm
        JOIN advogados a ON wm.advogado_id = a.id
        ORDER BY wm.data_envio DESC
        LIMIT 200
    """).fetchall()

    return render_template(
        "whatsapp.html",
        mensagens=mensagens,
        whatsapp_hoje=contar_whatsapp_hoje(),
        limite_whatsapp=LIMITE_DIARIO_WHATSAPP,
        vendedor=VENDEDOR,
    )


@app.route("/apresentacao/<int:id>")
def apresentacao(id):
    """Página de apresentação personalizada para enviar ao cliente."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        flash("Advogado não encontrado.", "erro")
        return redirect(url_for("listar_advogados"))

    abordagem = None
    if adv["abordagem_personalizada"]:
        try:
            abordagem = json.loads(adv["abordagem_personalizada"])
        except (json.JSONDecodeError, TypeError):
            pass

    areas = []
    if adv["areas_atuacao"]:
        try:
            areas = json.loads(adv["areas_atuacao"])
        except (json.JSONDecodeError, TypeError):
            pass

    return render_template(
        "apresentacao.html",
        adv=adv,
        abordagem=abordagem,
        areas=areas,
        vendedor=VENDEDOR,
    )


# ─── ROTAS — API REST ──────────────────────────────────────

@app.route("/api/advogados")
def api_listar_advogados():
    """API: Listar advogados com filtros."""
    db = get_db()

    status = request.args.get("status")
    estado = request.args.get("estado")
    score_min = request.args.get("score_min", type=int)
    limite = request.args.get("limite", 100, type=int)

    query = "SELECT * FROM advogados WHERE 1=1"
    params = []

    if status:
        query += " AND status = ?"
        params.append(status)
    if estado:
        query += " AND estado = ?"
        params.append(estado)
    if score_min:
        query += " AND score_potencial >= ?"
        params.append(score_min)

    query += " ORDER BY score_potencial DESC LIMIT ?"
    params.append(limite)

    rows = db.execute(query, params).fetchall()
    advogados = [dict(r) for r in rows]

    return jsonify({"ok": True, "total": len(advogados), "advogados": advogados})


@app.route("/api/advogado/<int:id>/email", methods=["POST"])
def api_enviar_email(id):
    """API: Enviar email para advogado."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        return jsonify({"ok": False, "erro": "Advogado não encontrado"}), 404

    if not adv["email"]:
        return jsonify({"ok": False, "erro": "Advogado não possui email cadastrado"}), 400

    if contar_emails_hoje() >= LIMITE_DIARIO_EMAILS:
        return jsonify({"ok": False, "erro": f"Limite diário de {LIMITE_DIARIO_EMAILS} emails atingido"}), 429

    tipo = request.json.get("tipo", "primeiro_contato") if request.is_json else "primeiro_contato"

    # Gerar abordagem se não existir
    abordagem = adv["abordagem_personalizada"]
    if not abordagem:
        ab = gerar_abordagem(dict(adv))
        abordagem = json.dumps(ab, ensure_ascii=False)
        db.execute(
            "UPDATE advogados SET abordagem_personalizada = ? WHERE id = ?",
            (abordagem, id),
        )
        db.commit()

    # Gerar email conforme tipo
    if tipo == "primeiro_contato":
        assunto, corpo_html, corpo_texto = gerar_email_primeiro_contato(adv, abordagem)
    elif tipo == "followup_4d":
        assunto, corpo_html, corpo_texto = gerar_email_followup(adv, abordagem)
    elif tipo == "final_14d":
        assunto, corpo_html, corpo_texto = gerar_email_final(adv, abordagem)
    else:
        return jsonify({"ok": False, "erro": f"Tipo de email inválido: {tipo}"}), 400

    # Tentar enviar via Gmail
    try:
        from gmail_service import enviar_email
        conta = request.json.get("conta") if request.is_json else None
        result = enviar_email(adv["email"], assunto, corpo_html, corpo_texto, conta)
        message_id = result.get("id", "")
        thread_id = result.get("threadId", "")
        status_envio = "enviado"
    except ImportError:
        logger.warning("gmail_service não disponível — salvando como rascunho")
        message_id = ""
        thread_id = ""
        status_envio = "rascunho"
    except Exception as e:
        logger.error(f"Erro ao enviar email: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500

    # Registrar no banco
    agora = datetime.now().isoformat()
    db.execute("""
        INSERT INTO emails_enviados (advogado_id, tipo, assunto, corpo_html, corpo_texto,
                                      data_envio, message_id, thread_id, status, conta_gmail)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (id, tipo, assunto, corpo_html, corpo_texto, agora, message_id, thread_id, status_envio, ""))
    
    if adv["status"] == "novo":
        db.execute("UPDATE advogados SET status = 'contatado', data_contato = ? WHERE id = ?", (agora, id))

    db.commit()
    registrar_historico(id, f"Email {tipo} enviado", f"Assunto: {assunto}")

    return jsonify({"ok": True, "status": status_envio, "message_id": message_id})


@app.route("/api/advogado/<int:id>/whatsapp", methods=["POST"])
def api_enviar_whatsapp(id):
    """API: Enviar WhatsApp para advogado."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        return jsonify({"ok": False, "erro": "Advogado não encontrado"}), 404

    if not adv["whatsapp"] and not adv["telefone"]:
        return jsonify({"ok": False, "erro": "Advogado não possui WhatsApp/telefone cadastrado"}), 400

    if contar_whatsapp_hoje() >= LIMITE_DIARIO_WHATSAPP:
        return jsonify({"ok": False, "erro": f"Limite diário de {LIMITE_DIARIO_WHATSAPP} mensagens atingido"}), 429

    tipo = request.json.get("tipo", "primeiro_contato") if request.is_json else "primeiro_contato"
    numero = adv["whatsapp"] or adv["telefone"]

    # Gerar abordagem se não existir
    abordagem = adv["abordagem_personalizada"]
    if not abordagem:
        ab = gerar_abordagem(dict(adv))
        abordagem = json.dumps(ab, ensure_ascii=False)
        db.execute(
            "UPDATE advogados SET abordagem_personalizada = ? WHERE id = ?",
            (abordagem, id),
        )
        db.commit()

    # Gerar mensagem
    if tipo == "primeiro_contato":
        mensagem = gerar_whatsapp_primeiro_contato(adv, abordagem)
    elif tipo == "followup":
        mensagem = gerar_whatsapp_followup(adv, abordagem)
    elif tipo == "final":
        mensagem = gerar_whatsapp_final(adv, abordagem)
    else:
        return jsonify({"ok": False, "erro": f"Tipo inválido: {tipo}"}), 400

    # Tentar enviar via WPPConnect
    status_envio = "pendente"
    try:
        from whatsapp_service import enviar_mensagem
        result = enviar_mensagem(numero, mensagem)
        status_envio = "enviado" if result else "erro"
    except ImportError:
        logger.warning("whatsapp_service não disponível — salvando como pendente")
    except Exception as e:
        logger.error(f"Erro ao enviar WhatsApp: {e}")

    agora = datetime.now().isoformat()
    db.execute("""
        INSERT INTO whatsapp_mensagens (advogado_id, tipo, mensagem, data_envio, status)
        VALUES (?, ?, ?, ?, ?)
    """, (id, tipo, mensagem, agora, status_envio))

    if adv["status"] == "novo":
        db.execute("UPDATE advogados SET status = 'contatado', data_contato = ? WHERE id = ?", (agora, id))

    db.commit()
    registrar_historico(id, f"WhatsApp {tipo} enviado", f"Status: {status_envio}")

    return jsonify({"ok": True, "status": status_envio})


@app.route("/api/advogado/<int:id>/status", methods=["POST"])
def api_atualizar_status(id):
    """API: Atualizar status do advogado."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        return jsonify({"ok": False, "erro": "Advogado não encontrado"}), 404

    novo_status = request.json.get("status") if request.is_json else request.form.get("status")
    if novo_status not in ("novo", "contatado", "interessado", "fechado", "perdido"):
        return jsonify({"ok": False, "erro": "Status inválido"}), 400

    db.execute("UPDATE advogados SET status = ? WHERE id = ?", (novo_status, id))
    db.commit()
    registrar_historico(id, "Status atualizado", f"{adv['status']} → {novo_status}")

    return jsonify({"ok": True, "status": novo_status})


@app.route("/api/advogado/<int:id>/abordagem", methods=["POST"])
def api_gerar_abordagem(id):
    """API: Gerar/regerar abordagem personalizada."""
    db = get_db()
    adv = db.execute("SELECT * FROM advogados WHERE id = ?", (id,)).fetchone()
    if not adv:
        return jsonify({"ok": False, "erro": "Advogado não encontrado"}), 404

    abordagem = gerar_abordagem(dict(adv))
    abordagem_json = json.dumps(abordagem, ensure_ascii=False)

    score = calcular_score(dict(adv))

    db.execute(
        "UPDATE advogados SET abordagem_personalizada = ?, score_potencial = ? WHERE id = ?",
        (abordagem_json, score, id),
    )
    db.commit()
    registrar_historico(id, "Abordagem gerada", f"Score: {score}")

    return jsonify({"ok": True, "abordagem": abordagem, "score": score})


@app.route("/api/email/contagem-hoje")
def api_contagem_emails():
    """API: Contagem de emails enviados hoje."""
    return jsonify({
        "ok": True,
        "emails_hoje": contar_emails_hoje(),
        "limite": LIMITE_DIARIO_EMAILS,
        "whatsapp_hoje": contar_whatsapp_hoje(),
        "limite_whatsapp": LIMITE_DIARIO_WHATSAPP,
    })


@app.route("/api/automacao/iniciar", methods=["POST"])
def api_iniciar_automacao():
    """API: Iniciar fila de automação para advogados novos com score alto."""
    db = get_db()

    score_minimo = request.json.get("score_minimo", 50) if request.is_json else 50
    limite = request.json.get("limite", 20) if request.is_json else 20

    advogados = db.execute("""
        SELECT id FROM advogados
        WHERE status = 'novo'
          AND tem_site = 0
          AND score_potencial >= ?
          AND (email IS NOT NULL OR whatsapp IS NOT NULL OR telefone IS NOT NULL)
        ORDER BY score_potencial DESC
        LIMIT ?
    """, (score_minimo, limite)).fetchall()

    agora = datetime.now()
    adicionados = 0

    for adv in advogados:
        adv_id = adv["id"]

        # Verificar se já tem na fila
        ja_existe = db.execute(
            "SELECT COUNT(*) FROM automacao_fila WHERE advogado_id = ? AND status = 'pendente'",
            (adv_id,),
        ).fetchone()[0]
        if ja_existe:
            continue

        # Email 1 — agora
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'primeiro_contato', ?, 'pendente')
        """, (adv_id, agora.isoformat()))

        # WhatsApp 1 — +1h
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'primeiro_contato', ?, 'pendente')
        """, (adv_id, (agora + timedelta(hours=1)).isoformat()))

        # Email 2 — +4 dias
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'followup_4d', ?, 'pendente')
        """, (adv_id, (agora + timedelta(days=4)).isoformat()))

        # WhatsApp 2 — +3 dias
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'followup', ?, 'pendente')
        """, (adv_id, (agora + timedelta(days=3)).isoformat()))

        # Email 3 — +14 dias
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'email', 'final_14d', ?, 'pendente')
        """, (adv_id, (agora + timedelta(days=14)).isoformat()))

        # WhatsApp 3 — +10 dias
        db.execute("""
            INSERT INTO automacao_fila (advogado_id, canal, tipo_mensagem, data_agendada, status)
            VALUES (?, 'whatsapp', 'final', ?, 'pendente')
        """, (adv_id, (agora + timedelta(days=10)).isoformat()))

        adicionados += 1

    db.commit()

    return jsonify({
        "ok": True,
        "adicionados": adicionados,
        "mensagem": f"{adicionados} advogados adicionados à fila de automação ({adicionados * 6} mensagens)"
    })


@app.route("/api/automacao/processar", methods=["POST"])
def api_processar_fila():
    """API: Processar fila de automação — envia mensagens agendadas."""
    db = get_db()
    agora = datetime.now().isoformat()

    pendentes = db.execute("""
        SELECT af.*, a.*
        FROM automacao_fila af
        JOIN advogados a ON af.advogado_id = a.id
        WHERE af.status = 'pendente' AND af.data_agendada <= ?
        ORDER BY af.data_agendada ASC
        LIMIT 20
    """, (agora,)).fetchall()

    enviados = 0
    erros = 0

    for item in pendentes:
        try:
            if item["canal"] == "email":
                if contar_emails_hoje() >= LIMITE_DIARIO_EMAILS:
                    break

                # Chamar a rota de envio internamente
                with app.test_request_context(json={"tipo": item["tipo_mensagem"]}):
                    # Simplificado: gerar e salvar
                    abordagem = item["abordagem_personalizada"]
                    if not abordagem:
                        ab = gerar_abordagem(dict(item))
                        abordagem = json.dumps(ab, ensure_ascii=False)

                    if item["tipo_mensagem"] == "primeiro_contato":
                        assunto, corpo_html, corpo_texto = gerar_email_primeiro_contato(item, abordagem)
                    elif item["tipo_mensagem"] == "followup_4d":
                        assunto, corpo_html, corpo_texto = gerar_email_followup(item, abordagem)
                    else:
                        assunto, corpo_html, corpo_texto = gerar_email_final(item, abordagem)

                    db.execute("""
                        INSERT INTO emails_enviados (advogado_id, tipo, assunto, corpo_html, corpo_texto, data_envio, status)
                        VALUES (?, ?, ?, ?, ?, ?, 'rascunho')
                    """, (item["advogado_id"], item["tipo_mensagem"], assunto, corpo_html, corpo_texto, agora))

            elif item["canal"] == "whatsapp":
                if contar_whatsapp_hoje() >= LIMITE_DIARIO_WHATSAPP:
                    break

                abordagem = item["abordagem_personalizada"]
                if not abordagem:
                    ab = gerar_abordagem(dict(item))
                    abordagem = json.dumps(ab, ensure_ascii=False)

                if item["tipo_mensagem"] == "primeiro_contato":
                    mensagem = gerar_whatsapp_primeiro_contato(item, abordagem)
                elif item["tipo_mensagem"] == "followup":
                    mensagem = gerar_whatsapp_followup(item, abordagem)
                else:
                    mensagem = gerar_whatsapp_final(item, abordagem)

                db.execute("""
                    INSERT INTO whatsapp_mensagens (advogado_id, tipo, mensagem, data_envio, status)
                    VALUES (?, ?, ?, ?, 'pendente')
                """, (item["advogado_id"], item["tipo_mensagem"], mensagem, agora))

            db.execute("UPDATE automacao_fila SET status = 'enviado' WHERE id = ?", (item["id"],))
            enviados += 1

        except Exception as e:
            logger.error(f"Erro ao processar fila item {item['id']}: {e}")
            db.execute("UPDATE automacao_fila SET status = 'erro' WHERE id = ?", (item["id"],))
            erros += 1

    db.commit()

    return jsonify({
        "ok": True,
        "enviados": enviados,
        "erros": erros,
        "mensagem": f"Processados: {enviados} enviados, {erros} erros"
    })


# ─── API Enriquecimento ────────────────────────────────────

@app.route("/api/advogado/<int:id>/enriquecer", methods=["POST"])
def api_enriquecer_advogado(id):
    """API: Enriquecer dados de um advogado especifico."""
    try:
        from enriquecer_advogados import enriquecer_advogado
        resultado = enriquecer_advogado(id)
        if resultado:
            return jsonify({"ok": True, **resultado})
        return jsonify({"ok": False, "erro": "Advogado não encontrado"}), 404
    except ImportError as ie:
        logger.error(f"Erro ao importar enriquecer_advogados: {ie}")
        return jsonify({"ok": False, "erro": "Módulo de enriquecimento não disponível"}), 500
    except Exception as e:
        logger.error(f"Erro ao enriquecer advogado {id}: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/enriquecer/todos", methods=["POST"])
def api_enriquecer_todos():
    """API: Enriquecer todos os advogados que precisam."""
    try:
        from enriquecer_advogados import enriquecer_todos
        limite = 50
        if request.is_json and request.json:
            limite = request.json.get("limite", 50)
        resultado = enriquecer_todos(limite=limite)
        return jsonify({"ok": True, **resultado})
    except ImportError as ie:
        logger.error(f"Erro ao importar enriquecer_advogados: {ie}")
        return jsonify({"ok": False, "erro": "Módulo de enriquecimento não disponível"}), 500
    except Exception as e:
        logger.error(f"Erro ao enriquecer todos: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500


# ─── API Prospecção ─────────────────────────────────────────

@app.route("/api/prospectar", methods=["POST"])
def api_prospectar():
    """API: Prospectar novos escritórios com verificação integrada de site."""
    try:
        from prospectar_advogados import prospectar_escritorios_reais
        n = 10
        if request.is_json and request.json:
            n = request.json.get("quantidade", 10)
        resultado = prospectar_escritorios_reais(n)
        return jsonify({"ok": True, **resultado})
    except ImportError as ie:
        logger.error(f"Erro ao importar prospectar_advogados: {ie}")
        return jsonify({"ok": False, "erro": "Módulo de prospecção não disponível"}), 500
    except Exception as e:
        logger.error(f"Erro ao prospectar: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/verificar-site", methods=["POST"])
def api_verificar_site():
    """API: Verificar se um escritório tem site (teste rápido)."""
    try:
        from prospectar_advogados import verificar_site_completo
        data = request.get_json()
        nome = data.get("nome", "")
        nome_escritorio = data.get("nome_escritorio", nome)
        cidade = data.get("cidade", "")
        estado = data.get("estado", "")

        resultado = verificar_site_completo(nome, nome_escritorio, cidade, estado)
        return jsonify({"ok": True, **resultado})
    except Exception as e:
        logger.error(f"Erro ao verificar site: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/limpar-banco", methods=["POST"])
def api_limpar_banco():
    """API: Limpar todos os dados do banco para recomeco."""
    try:
        from prospectar_advogados import limpar_banco
        limpar_banco()
        return jsonify({"ok": True, "mensagem": "Banco limpo com sucesso"})
    except Exception as e:
        logger.error(f"Erro ao limpar banco: {e}")
        return jsonify({"ok": False, "erro": str(e)}), 500


@app.route("/api/dashboard/stats")
def api_stats():
    """API: Estatísticas gerais para dashboard."""
    db = get_db()

    stats = {
        "total_leads": db.execute("SELECT COUNT(*) FROM advogados").fetchone()[0],
        "sem_site": db.execute("SELECT COUNT(*) FROM advogados WHERE tem_site = 0").fetchone()[0],
        "novos": db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'novo'").fetchone()[0],
        "contatados": db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'contatado'").fetchone()[0],
        "interessados": db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'interessado'").fetchone()[0],
        "fechados": db.execute("SELECT COUNT(*) FROM advogados WHERE status = 'fechado'").fetchone()[0],
        "emails_hoje": contar_emails_hoje(),
        "whatsapp_hoje": contar_whatsapp_hoje(),
        "total_respostas": db.execute("SELECT COUNT(*) FROM respostas").fetchone()[0],
        "fila_pendente": db.execute("SELECT COUNT(*) FROM automacao_fila WHERE status = 'pendente'").fetchone()[0],
    }

    return jsonify({"ok": True, **stats})


# ─── Gmail OAuth ───────────────────────────────────────────

@app.route("/gmail/conectar/<int:conta_id>")
def gmail_conectar(conta_id):
    """Iniciar fluxo OAuth2 para conta Gmail."""
    try:
        from gmail_service import iniciar_oauth
        auth_url = iniciar_oauth(conta_id)
        return redirect(auth_url)
    except ImportError:
        flash("Serviço Gmail não configurado. Adicione credentials.json.", "erro")
        return redirect(url_for("automacao"))
    except Exception as e:
        flash(f"Erro ao conectar Gmail: {e}", "erro")
        return redirect(url_for("automacao"))


@app.route("/oauth2callback")
def oauth2callback():
    """Callback do OAuth2 Gmail."""
    try:
        from gmail_service import processar_callback
        result = processar_callback(request.url)
        flash(f"Gmail conectado com sucesso: {result['email']}", "sucesso")
    except ImportError:
        flash("Serviço Gmail não configurado.", "erro")
    except Exception as e:
        flash(f"Erro no callback: {e}", "erro")

    return redirect(url_for("automacao"))


# ─── Seed de dados de exemplo ──────────────────────────────
def seed_exemplo():
    """Insere dados de exemplo para demonstração."""
    db = sqlite3.connect(app.config["DATABASE"])

    count = db.execute("SELECT COUNT(*) FROM advogados").fetchone()[0]
    if count > 0:
        db.close()
        return

    exemplos = [
        {
            "nome": "Dr. Carlos Eduardo Mendes",
            "nome_escritorio": "Mendes Advocacia",
            "numero_oab": "345.678",
            "seccional_oab": "SP",
            "situacao_oab": "Ativo",
            "email": "carlos.mendes@example.com",
            "telefone": "(11) 98765-4321",
            "whatsapp": "5511987654321",
            "cidade": "São Paulo",
            "estado": "SP",
            "tem_site": 0,
            "instagram": "@mendesadvocacia",
            "instagram_seguidores": 2340,
            "google_avaliacao": 4.7,
            "google_reviews": 28,
            "areas_atuacao": '["Direito Trabalhista", "Direito Civil", "Direito do Consumidor"]',
            "tempo_atuacao": 12,
            "volume_processos": 45,
            "porte_escritorio": "Pequeno",
            "fonte": "google_maps",
        },
        {
            "nome": "Dra. Ana Beatriz Rocha",
            "nome_escritorio": "Rocha & Associados",
            "numero_oab": "234.567",
            "seccional_oab": "RJ",
            "situacao_oab": "Ativo",
            "email": "ana.rocha@example.com",
            "telefone": "(21) 97654-3210",
            "whatsapp": "5521976543210",
            "cidade": "Rio de Janeiro",
            "estado": "RJ",
            "tem_site": 0,
            "instagram": "@rochaadvogados",
            "instagram_seguidores": 1890,
            "facebook": "Rocha & Associados Advocacia",
            "facebook_seguidores": 450,
            "google_avaliacao": 4.5,
            "google_reviews": 15,
            "areas_atuacao": '["Direito Criminal", "Direito Penal", "Direito de Família"]',
            "tempo_atuacao": 8,
            "volume_processos": 32,
            "porte_escritorio": "Pequeno",
            "fonte": "google_maps",
        },
        {
            "nome": "Dr. Roberto Ferreira Lima",
            "nome_escritorio": None,
            "numero_oab": "456.789",
            "seccional_oab": "MG",
            "situacao_oab": "Ativo",
            "email": "roberto.lima@example.com",
            "telefone": "(31) 96543-2100",
            "whatsapp": "5531965432100",
            "cidade": "Belo Horizonte",
            "estado": "MG",
            "tem_site": 0,
            "instagram": None,
            "google_avaliacao": 4.2,
            "google_reviews": 8,
            "areas_atuacao": '["Direito Previdenciário", "Direito Trabalhista"]',
            "tempo_atuacao": 15,
            "volume_processos": 78,
            "porte_escritorio": "Solo",
            "fonte": "oab_cna",
        },
        {
            "nome": "Dra. Mariana Costa Santos",
            "nome_escritorio": "Costa Santos Advogados",
            "numero_oab": "567.890",
            "seccional_oab": "PR",
            "situacao_oab": "Ativo",
            "email": "mariana.costa@example.com",
            "telefone": "(41) 95432-1098",
            "whatsapp": "5541954321098",
            "cidade": "Curitiba",
            "estado": "PR",
            "tem_site": 0,
            "instagram": "@costasantosadv",
            "instagram_seguidores": 3200,
            "facebook": "Costa Santos Advogados",
            "facebook_seguidores": 780,
            "linkedin": "mariana-costa-santos-adv",
            "google_avaliacao": 4.9,
            "google_reviews": 42,
            "areas_atuacao": '["Direito Empresarial", "Direito Tributário", "Direito Civil", "Contratos"]',
            "tempo_atuacao": 10,
            "volume_processos": 25,
            "porte_escritorio": "Pequeno",
            "fonte": "google_maps",
        },
        {
            "nome": "Dr. Paulo Henrique Oliveira",
            "nome_escritorio": "Oliveira Advocacia Criminal",
            "numero_oab": "678.901",
            "seccional_oab": "BA",
            "situacao_oab": "Ativo",
            "email": "paulo.oliveira@example.com",
            "telefone": "(71) 94321-0987",
            "whatsapp": "5571943210987",
            "cidade": "Salvador",
            "estado": "BA",
            "tem_site": 0,
            "google_avaliacao": 4.3,
            "google_reviews": 11,
            "areas_atuacao": '["Direito Criminal", "Direito Penal"]',
            "tempo_atuacao": 20,
            "volume_processos": 120,
            "porte_escritorio": "Solo",
            "fonte": "google_maps",
        },
    ]

    for ex in exemplos:
        cols = ", ".join(ex.keys())
        placeholders = ", ".join(["?"] * len(ex))
        db.execute(f"INSERT INTO advogados ({cols}) VALUES ({placeholders})", list(ex.values()))

    db.commit()

    # Calcular scores
    db.row_factory = sqlite3.Row
    rows = db.execute("SELECT * FROM advogados").fetchall()
    for row in rows:
        score = calcular_score(dict(row))
        db.execute("UPDATE advogados SET score_potencial = ? WHERE id = ?", (score, row["id"]))

    db.commit()
    db.close()
    logger.info("Dados de exemplo inseridos com sucesso.")


# ─── Inicialização ─────────────────────────────────────────
init_db()
seed_exemplo()


if __name__ == "__main__":
    app.run(debug=True, port=5050, host="0.0.0.0", use_reloader=False)
