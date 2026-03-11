"""
Validador de Contatos — ProspectAdv Pipeline
Verifica se telefone e email sao reais e ativos.

Tecnicas:
1. Telefone: formato BR, DDD valido, WhatsApp lookup via API
2. Email: sintaxe, MX record, dominio ativo, disposable check
3. Pixel tracking: gera pixel 1x1 para rastrear abertura
4. Flags: valid_phone, valid_email, contact_ok
"""

import re
import dns.resolver
import socket
import smtplib
import logging
import uuid
from urllib.parse import quote

logger = logging.getLogger("ProspectAdv.Validador")

# ============================================================
# 1. VALIDACAO DE TELEFONE BR
# ============================================================

# DDDs validos do Brasil (todos os estados)
DDDS_VALIDOS = {
    # SP
    11, 12, 13, 14, 15, 16, 17, 18, 19,
    # RJ
    21, 22, 24,
    # ES
    27, 28,
    # MG
    31, 32, 33, 34, 35, 37, 38,
    # PR
    41, 42, 43, 44, 45, 46,
    # SC
    47, 48, 49,
    # RS
    51, 53, 54, 55,
    # DF/GO/TO/MT/MS
    61, 62, 63, 64, 65, 66, 67, 68, 69,
    # BA/SE
    71, 73, 74, 75, 77, 79,
    # PE/AL/PB/RN/CE/PI/MA/PA/AP/AM/RR/AC
    81, 82, 83, 84, 85, 86, 87, 88, 89,
    91, 92, 93, 94, 95, 96, 97, 98, 99,
}


def limpar_telefone(telefone):
    """Remove formatacao, retorna apenas digitos."""
    if not telefone:
        return None
    return re.sub(r"\D", "", str(telefone))


def formatar_telefone_br(telefone):
    """Formata telefone brasileiro: (XX) XXXXX-XXXX ou (XX) XXXX-XXXX."""
    digitos = limpar_telefone(telefone)
    if not digitos:
        return None

    # Remover +55 se presente
    if digitos.startswith("55") and len(digitos) >= 12:
        digitos = digitos[2:]

    if len(digitos) == 11:
        return f"({digitos[:2]}) {digitos[2:7]}-{digitos[7:]}"
    elif len(digitos) == 10:
        return f"({digitos[:2]}) {digitos[2:6]}-{digitos[6:]}"
    return telefone


def validar_telefone_br(telefone):
    """
    Valida telefone brasileiro.
    Retorna dict: {valido, tipo, ddd, numero_limpo, formatado, motivo}
    """
    resultado = {
        "valido": False,
        "tipo": None,
        "ddd": None,
        "numero_limpo": None,
        "numero_full": None,
        "formatado": None,
        "motivo": "",
    }

    digitos = limpar_telefone(telefone)
    if not digitos:
        resultado["motivo"] = "vazio"
        return resultado

    # Remover codigo pais (+55)
    if digitos.startswith("55") and len(digitos) >= 12:
        digitos = digitos[2:]

    resultado["numero_limpo"] = digitos
    resultado["numero_full"] = f"55{digitos}"

    # Validar tamanho (10 fixo, 11 celular)
    if len(digitos) not in (10, 11):
        resultado["motivo"] = f"tamanho invalido ({len(digitos)} digitos)"
        return resultado

    # Validar DDD
    ddd = int(digitos[:2])
    if ddd not in DDDS_VALIDOS:
        resultado["motivo"] = f"DDD invalido ({ddd})"
        return resultado
    resultado["ddd"] = ddd

    # Validar prefixo
    numero = digitos[2:]
    if len(digitos) == 11:
        # Celular: deve comecar com 9
        if numero[0] != "9":
            resultado["motivo"] = "celular deve comecar com 9"
            return resultado
        # Segundo digito nao pode ser 0
        if numero[1] == "0":
            resultado["motivo"] = "segundo digito invalido"
            return resultado
        resultado["tipo"] = "celular"
    else:
        # Fixo: primeiro digito 2-5
        if numero[0] not in "2345":
            resultado["motivo"] = "fixo deve comecar com 2-5"
            return resultado
        resultado["tipo"] = "fixo"

    # Rejeitar numeros repetidos (ex: 99999999)
    if len(set(numero)) <= 1:
        resultado["motivo"] = "numero repetido"
        return resultado

    resultado["valido"] = True
    resultado["formatado"] = formatar_telefone_br(digitos)
    resultado["motivo"] = "formato valido"
    return resultado


def verificar_whatsapp(telefone, wpp_api_url=None, wpp_secret=None):
    """
    Verifica se telefone tem WhatsApp ativo via WPPConnect API.

    Args:
        telefone: Numero com DDI (5541999999999)
        wpp_api_url: URL da API WPPConnect
        wpp_secret: Secret key

    Returns:
        dict: {exists, number, is_business}
    """
    import requests

    resultado = {"exists": False, "number": telefone, "is_business": False}

    if not wpp_api_url:
        logger.debug("WPPConnect API nao configurada")
        return resultado

    digitos = limpar_telefone(telefone)
    if not digitos:
        return resultado

    # Garantir DDI
    if not digitos.startswith("55"):
        digitos = f"55{digitos}"

    try:
        resp = requests.post(
            f"{wpp_api_url}/api/default/check-number-status/{digitos}@c.us",
            headers={
                "Authorization": f"Bearer {wpp_secret}",
                "Content-Type": "application/json",
            },
            timeout=10,
        )

        if resp.status_code == 200:
            data = resp.json()
            resultado["exists"] = data.get("result", {}).get("numberExists", False)
            resultado["is_business"] = data.get("result", {}).get("isBusiness", False)
            resultado["number"] = digitos

    except Exception as e:
        logger.debug(f"Erro WhatsApp check: {e}")

    return resultado


# ============================================================
# 2. VALIDACAO DE EMAIL
# ============================================================

# Dominios descartaveis conhecidos (parcial, expandir conforme necessidade)
DOMINIOS_DESCARTAVEIS = {
    "tempmail.com", "throwaway.email", "guerrillamail.com",
    "mailinator.com", "yopmail.com", "sharklasers.com",
    "guerrillamailblock.com", "grr.la", "dispostable.com",
    "trashmail.com", "10minutemail.com", "temp-mail.org",
    "fakeinbox.com", "mailnesia.com", "maildrop.cc",
    "getnada.com", "emailondeck.com", "tempr.email",
    "33mail.com", "mytrashmail.com",
}

# Dominios de email profissional (provavelmente validos)
DOMINIOS_PROFISSIONAIS = {
    "gmail.com", "outlook.com", "hotmail.com", "yahoo.com",
    "yahoo.com.br", "bol.com.br", "uol.com.br", "terra.com.br",
    "ig.com.br", "globo.com", "globomail.com",
}


def validar_sintaxe_email(email):
    """Valida sintaxe basica do email."""
    if not email:
        return False
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    return bool(re.match(pattern, email))


def verificar_mx(dominio, timeout=5):
    """
    Verifica se dominio tem registros MX (aceita email).
    Retorna lista de servidores MX ou None.
    """
    try:
        resolver = dns.resolver.Resolver()
        resolver.timeout = timeout
        resolver.lifetime = timeout
        records = resolver.resolve(dominio, "MX")
        return [str(r.exchange).rstrip(".") for r in records]
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN,
            dns.resolver.NoNameservers, dns.exception.Timeout):
        return None
    except Exception:
        return None


def verificar_smtp(email, mx_server, timeout=10):
    """
    Tenta verificar email via SMTP RCPT TO (nao envia email).
    Muitos servidores bloqueiam — usar como sinal extra, nao definitivo.

    Returns: True (aceito), False (rejeitado), None (inconclusivo)
    """
    try:
        smtp = smtplib.SMTP(mx_server, 25, timeout=timeout)
        smtp.ehlo("prospectadv.com.br")
        smtp.mail("verificacao@prospectadv.com.br")
        code, _ = smtp.rcpt(email)
        smtp.quit()

        if code == 250:
            return True
        elif code >= 500:
            return False
        return None

    except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError,
            ConnectionRefusedError, socket.timeout, OSError):
        return None
    except Exception:
        return None


def validar_email_completo(email):
    """
    Validacao completa de email: sintaxe + MX + disposable check.
    Retorna dict: {valido, motivo, mx_servers, is_professional, is_disposable}
    """
    resultado = {
        "valido": False,
        "motivo": "",
        "mx_servers": [],
        "is_professional": False,
        "is_disposable": False,
        "smtp_check": None,
    }

    if not email:
        resultado["motivo"] = "vazio"
        return resultado

    email = email.strip().lower()

    # Sintaxe
    if not validar_sintaxe_email(email):
        resultado["motivo"] = "sintaxe invalida"
        return resultado

    # Extrair dominio
    dominio = email.split("@")[1]

    # Disposable check
    if dominio in DOMINIOS_DESCARTAVEIS:
        resultado["is_disposable"] = True
        resultado["motivo"] = "email temporario/descartavel"
        return resultado

    # Profissional check
    resultado["is_professional"] = dominio not in DOMINIOS_PROFISSIONAIS
    # Dominio proprio = mais provavel de ser real

    # MX records
    mx_servers = verificar_mx(dominio)
    if mx_servers:
        resultado["mx_servers"] = mx_servers
    else:
        resultado["motivo"] = "dominio sem MX (nao recebe email)"
        return resultado

    # SMTP check (opcional, muitos bloqueiam)
    if mx_servers:
        smtp_result = verificar_smtp(email, mx_servers[0])
        resultado["smtp_check"] = smtp_result
        if smtp_result is False:
            resultado["motivo"] = "email rejeitado pelo servidor"
            return resultado

    resultado["valido"] = True
    resultado["motivo"] = "email valido (sintaxe + MX OK)"
    return resultado


# ============================================================
# 3. PIXEL DE RASTREAMENTO (Email Open Tracking)
# ============================================================

def gerar_pixel_tracking(advogado_id, base_url="https://prospectadv.com.br"):
    """
    Gera URL de pixel 1x1 transparente para rastreio de abertura.
    Quando o email client carrega a imagem, registra abertura.

    Args:
        advogado_id: ID do advogado no banco
        base_url: URL base do servidor

    Returns:
        dict: {pixel_url, tracking_id, html_tag}
    """
    tracking_id = str(uuid.uuid4())[:8]
    pixel_url = f"{base_url}/api/track/open/{advogado_id}/{tracking_id}"

    # Tag HTML para inserir no corpo do email
    html_tag = (
        f'<img src="{pixel_url}" width="1" height="1" '
        f'alt="" style="display:none;border:0;" />'
    )

    return {
        "pixel_url": pixel_url,
        "tracking_id": tracking_id,
        "html_tag": html_tag,
    }


# ============================================================
# 4. VALIDACAO COMBINADA
# ============================================================

def validar_contato_completo(telefone=None, email=None, wpp_api_url=None, wpp_secret=None):
    """
    Validacao completa de contato: telefone + email + WhatsApp.
    Retorna dict com flags para CSV de saida.

    Output flags:
        valid_phone (0/1), valid_email (0/1), contact_ok (0/1)
    """
    resultado = {
        "telefone_full": None,
        "telefone_formatado": None,
        "valid_phone": 0,
        "phone_tipo": None,
        "whatsapp_exists": False,
        "email_validado": None,
        "valid_email": 0,
        "email_professional": False,
        "contact_ok": 0,
    }

    # === Telefone ===
    if telefone:
        tel_result = validar_telefone_br(telefone)
        resultado["telefone_full"] = tel_result["numero_full"]
        resultado["telefone_formatado"] = tel_result["formatado"]
        resultado["valid_phone"] = 1 if tel_result["valido"] else 0
        resultado["phone_tipo"] = tel_result["tipo"]

        # WhatsApp check (se API configurada)
        if tel_result["valido"] and wpp_api_url:
            wpp = verificar_whatsapp(tel_result["numero_full"], wpp_api_url, wpp_secret)
            resultado["whatsapp_exists"] = wpp["exists"]

    # === Email ===
    if email:
        email_result = validar_email_completo(email)
        resultado["email_validado"] = email if email_result["valido"] else None
        resultado["valid_email"] = 1 if email_result["valido"] else 0
        resultado["email_professional"] = email_result["is_professional"]

    # === Flag combinada ===
    # contact_ok = pelo menos um canal de contato validado
    if resultado["valid_phone"] or resultado["valid_email"]:
        resultado["contact_ok"] = 1

    return resultado


# ============================================================
# 5. CNPJ — Busca na Receita Federal (dados publicos)
# ============================================================

def buscar_cnpj_por_nome(nome_escritorio, estado=None):
    """
    Busca CNPJ de escritorio de advocacia por nome.
    Usa APIs publicas (ReceitaWS, BrasilAPI).

    Returns:
        dict: {cnpj, razao_social, endereco, situacao} ou None
    """
    import requests

    if not nome_escritorio:
        return None

    # Tentar BrasilAPI primeiro (mais permissiva)
    try:
        # BrasilAPI nao tem busca por nome diretamente
        # Usar Google para achar o CNPJ
        from urllib.parse import quote_plus
        query = f'"{nome_escritorio}" CNPJ site:cnpj.biz OR site:casadosdados.com.br'
        resp = requests.get(
            f"https://www.google.com.br/search?q={quote_plus(query)}&hl=pt-BR&num=5",
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept-Language": "pt-BR,pt;q=0.9",
            },
            timeout=10,
        )

        if resp.status_code == 200:
            # Extrair CNPJs do resultado
            cnpjs = re.findall(
                r"\d{2}\.?\d{3}\.?\d{3}/?\d{4}-?\d{2}",
                resp.text
            )
            if cnpjs:
                cnpj_limpo = re.sub(r"\D", "", cnpjs[0])
                # Consultar dados do CNPJ
                return consultar_cnpj(cnpj_limpo)

    except Exception as e:
        logger.debug(f"Erro buscando CNPJ: {e}")

    return None


def consultar_cnpj(cnpj):
    """
    Consulta dados de CNPJ via BrasilAPI (gratuita).
    """
    import requests

    cnpj_limpo = re.sub(r"\D", "", str(cnpj))
    if len(cnpj_limpo) != 14:
        return None

    try:
        resp = requests.get(
            f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}",
            timeout=10,
        )

        if resp.status_code == 200:
            data = resp.json()
            endereco_parts = [
                data.get("logradouro", ""),
                data.get("numero", ""),
                data.get("complemento", ""),
                data.get("bairro", ""),
                data.get("municipio", ""),
                data.get("uf", ""),
                data.get("cep", ""),
            ]
            endereco = ", ".join(p for p in endereco_parts if p)

            return {
                "cnpj": cnpj_limpo,
                "cnpj_formatado": f"{cnpj_limpo[:2]}.{cnpj_limpo[2:5]}.{cnpj_limpo[5:8]}/{cnpj_limpo[8:12]}-{cnpj_limpo[12:]}",
                "razao_social": data.get("razao_social", ""),
                "nome_fantasia": data.get("nome_fantasia", ""),
                "endereco": endereco,
                "situacao": data.get("descricao_situacao_cadastral", ""),
                "telefone": data.get("ddd_telefone_1", ""),
                "email": data.get("email", ""),
                "atividade_principal": data.get("cnae_fiscal_descricao", ""),
            }

    except Exception as e:
        logger.debug(f"Erro consultando CNPJ {cnpj}: {e}")

    return None


# ============================================================
# CLI
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    print("=== Teste Validador de Contatos ===\n")

    # Teste telefone
    testes_tel = [
        "(41) 99876-5432",
        "(11) 98765-4321",
        "(00) 12345-6789",  # DDD invalido
        "5541999887766",    # Com DDI
        "1234",             # Muito curto
    ]
    for tel in testes_tel:
        r = validar_telefone_br(tel)
        status = "OK" if r["valido"] else "FAIL"
        print(f"  [{status}] {tel} -> {r['formatado'] or r['motivo']}")

    print()

    # Teste email
    testes_email = [
        "contato@escritorio.adv.br",
        "advogado@gmail.com",
        "fake@tempmail.com",
        "invalido",
        "user@dominio-inexistente-xyz-abc.com.br",
    ]
    for em in testes_email:
        r = validar_email_completo(em)
        status = "OK" if r["valido"] else "FAIL"
        print(f"  [{status}] {em} -> {r['motivo']}")
