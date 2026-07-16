"""
Download de Tabelas de Domínio — DATASUS FTP + IBGE
Localização: sihrd5/src/data/extract/download_domain_tables.py

Fontes:
  1. ftp://ftp.datasus.gov.br/.../TAB_SIH.zip        — tabelas de domínio SIH
  2. servicodados.ibge.gov.br/api/v1/localidades/...  — códigos, nomes, UFs (IBGE)
  3. servicodados.ibge.gov.br/api/v3|v2/malhas/...  — centroides oficiais (IBGE)
"""

import ftplib, csv, sys, struct, logging, zipfile, requests, time, re, io
from pathlib import Path
from io import BytesIO

SRC_DIR = Path(__file__).parent.parent.parent  # src/
sys.path.insert(0, str(SRC_DIR))
from config.settings import Settings

# logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

FTP_HOST = 'ftp.datasus.gov.br'
FTP_PATH = '/dissemin/publicos/SIHSUS/200801_/Auxiliar/TAB_SIH.zip'
FTP_PATH_CNES_TAB = '/dissemin/publicos/CNES/200508_/Auxiliar/TAB_CNES.zip'
CNV_ESP_LEIT = 'CNV/Esp_leit.CNV'

TABELAS_ALVO = {
    'CARATEND' : ('car_int.csv',        'CAR_INT',       'DESCRICAO'),
    'MARCAUTI' : ('marca_uti.csv',      'MARCA_UTI',     'DESCRICAO'),
    'COMPLEX2' : ('complexidade.csv',   'COMPLEX',       'DESCRICAO'),
    'CBO'      : ('cbor.csv',           'CBOR',          'DESCRICAO'),
    'ESPEC'    : ('especialidade.csv',  'ESPEC',         'DESCRICAO'),
    'RACACOR'  : ('raca_cor.csv',       'RACA_COR',      'DESCRICAO'),
    'SEXO'     : ('sexo.csv',           'SEXO',          'DESCRICAO'),
    'INSTRU'   : ('instrucao.csv',      'INSTRU',        'DESCRICAO'),
    'VINCPREV' : ('vincprev.csv',       'VINCPREV',      'DESCRICAO'),
    'NACION3D' : ('nacionalidade.csv',  'NACIONAL',      'DESCRICAO'),
    'CONTRAC'  : ('contraceptivos.csv', 'CONTRACEPTIVO', 'DESCRICAO'),
}

# Tabelas com lógica de extração especial (não seguem o padrão simples codigo/descricao)
TABELAS_ESPECIAIS = {
    'CADHOSP'    : 'cadhosp.csv',         # TD_HOSPITAL — colunas: CGC_HOSP, RAZAO, UF_ZI, CMPT
    'TB_SIGTAP'  : 'procedimentos.csv',   # TD_PROCEDIMENTO — colunas: CO_PROCED, DS_PROCED
    'S_CID'      : 'cid.csv',            # TD_CID — fonte primária: código, descrição, hierarquia
    'etnia'      : 'etnia.csv',           # TD_ETNIA — CNV com código/descrição
    'br_regsaud' : 'regsaud.csv',         # TD_MUNICIPIO.NO_REGIAO_SAUDE — CNV código município → região saúde
    'TCNESBR'    : 'tcnes.csv',           # TD_HOSPITAL — colunas: CNES, NOMEFANT 
}

# ---------------------------------------------------------------------------
# FIX 1: Fallback ESPEC — Especialidade do Leito
# ESPEC.cnv NÃO existe no TAB_SIH.zip. A tabela é fixa, documentada apenas
# nos manuais técnicos do SIH/SUS (IT DRAC nº 003/2012, Portaria SAS 743/2005
# e atualizações). Os 38 valores abaixo cobrem todos os códigos usados no SIH.
# ---------------------------------------------------------------------------
# LEGADO — substituído pelo download dinâmico de CNV/Esp_leit.CNV (TAB_CNES.zip).
# _ESPEC_HARDCODED = [
#     ("01", "Cirúrgico"),
#     ("02", "Obstétrico"),
#     ("03", "Clínico"),
#     ("04", "Crônicos"),
#     ("05", "Psiquiatria"),
#     ("06", "Pneumologia Sanitária"),
#     ("07", "Pediátrico"),
#     ("08", "Reabilitação"),
#     ("09", "Leito Dia / Cirúrgico"),
#     ("10", "Leito Dia / AIDS"),
#     ("11", "Leito Dia / Fibrose Cística"),
#     ("12", "Leito Dia / Intercorrência Pós-Transplante"),
#     ("13", "Leito Dia / Geriatria"),
#     ("14", "Leito Dia / Saúde Mental"),
#
#     ("64", "Unidade Intermediária"),
#     ("65", "Unidade Intermediária Neonatal"),
#     ("74", "UTI Adulto - Tipo I"),
#     ("75", "UTI Adulto - Tipo II"),
#     ("76", "UTI Adulto - Tipo III"),
#     ("77", "UTI Infantil - Tipo I"),
#     ("78", "UTI Infantil - Tipo II"),
#     ("79", "UTI Infantil - Tipo III"),
#     ("80", "UTI Neonatal - Tipo I"),
#     ("81", "UTI Neonatal - Tipo II"),
#     ("82", "UTI Neonatal - Tipo III"),
#     ("83", "UTI de Queimados"),
#     ("85", "UTI Coronariana tipo II - UCO tipo II"),
#     ("86", "UTI Coronariana tipo III - UCO tipo III"),
#     ("87", "Saúde Mental - Longa Permanência"),
#     ("88", "Queimado Clínico Adulto"),
#     ("89", "Queimado Clínico Pediátrico"),
#     ("90", "Queimado Cirúrgico Adulto"),
#     ("91", "Queimado Cirúrgico Pediátrico"),
#     ("92", "UCI Neonatal Convencional"),
#     ("93", "UCI Neonatal Canguru"),
#     ("94", "UCI Pediátrica"),
#     ("95", "UCI Adulto"),
# ]


def _baixar_esp_leit_cnv() -> bytes:
    """Baixa TAB_CNES.zip do FTP DATASUS e extrai CNV/Esp_leit.CNV.
    Retorna o conteúdo binário do CNV, ou None em caso de falha.
    """
    try:
        logger.info(f"  Conectando FTP CNES: {FTP_HOST}")
        ftp = ftplib.FTP(FTP_HOST, timeout=120)
        ftp.login()
        buf = BytesIO()
        logger.info(f"  Baixando: {FTP_PATH_CNES_TAB}")
        ftp.retrbinary(f'RETR {FTP_PATH_CNES_TAB}', buf.write)
        ftp.quit()
        logger.info(f"  TAB_CNES.zip OK: {buf.tell() / 1024:.1f} KB")
        buf.seek(0)
        with zipfile.ZipFile(buf) as z:
            candidatos = [
                n for n in z.namelist()
                if n.replace('\\', '/').upper() == CNV_ESP_LEIT.upper()
            ]
            if not candidatos:
                logger.warning(
                    f"  {CNV_ESP_LEIT} não encontrado no ZIP. "
                    f"Arquivos disponíveis: {z.namelist()}"
                )
                return None
            conteudo = z.read(candidatos[0])
            logger.info(f"  Extraído: {candidatos[0]} ({len(conteudo)} bytes)")
            return conteudo
    except Exception as e:
        logger.warning(f"  Falha ao baixar Esp_leit.CNV do FTP CNES: {e}")
        return None


def _gerar_especialidade_fallback(caminho: Path) -> bool:
    """Gera especialidade.csv a partir de CNV/Esp_leit.CNV (TAB_CNES.zip, FTP DATASUS).
    Retorna False se o download ou parse falhar.
    """
    conteudo_cnv = _baixar_esp_leit_cnv()
    if conteudo_cnv is None:
        logger.error("  Falha ao obter Esp_leit.CNV — especialidade.csv não gerado")
        return False
    registros = parse_cnv(conteudo_cnv)
    if not registros:
        logger.error("  parse_cnv retornou vazio — especialidade.csv não gerado")
        return False
    ok = salvar_csv(registros, caminho, 'ESPEC', 'DESCRICAO')
    if ok:
        logger.info(f"  {caminho.name:<25s} {len(registros):>6,} registros (Esp_leit.CNV / FTP CNES)")
    return ok


def baixar_zip() -> bytes:
    logger.info(f"Conectando: {FTP_HOST}")
    ftp = ftplib.FTP(FTP_HOST, timeout=120)
    ftp.login()
    logger.info(f"Baixando: {FTP_PATH}")
    buf = BytesIO()
    ftp.retrbinary(f'RETR {FTP_PATH}', buf.write)
    ftp.quit()
    logger.info(f"Download OK: {buf.tell()/(1024*1024):.1f} MB")
    return buf.getvalue()


def parse_cnv(conteudo: bytes) -> list:
    """
    Formato CNV (TabWin) — dois subformatos detectados empiricamente:

    Header (linha 0): "<n_reg> <tam_chave> [L]"   ← sempre pulada

    Formato A — código aparece duas vezes (pos[1] == pos[-1]):
        "  <pos>  <CODIGO>  <DESCRICAO>   <CODIGO>"
        ex: "  1  00 Não utilizou UTI   00"
        → código = partes[1], descrição = partes[2:-1]
        Ocorre em: MARCAUTI, CARATEND

    Formato B — código aparece só no final:
        "  <pos>  <DESCRICAO>   <CODIGO_ou_faixa>"
        ex: "  1  Masculino   1"  |  "  6  Sem informação   00-99"
        → código = partes[-1], descrição = partes[1:-1]
        Ocorre em: SEXO, INSTRU, VINCPREV, RACACOR, COMPLEX2, CONTRAC, NACION3D

    Linhas com código contendo '-' são faixas agregadas TabWin — puladas.
    Linhas com código contendo ',' são múltiplos códigos para o mesmo rótulo
    (ex: "2,3" = Feminino) — expandidas em um registro por código.
    """
    registros = []
    try:
        linhas = conteudo.decode('latin-1', errors='replace').strip().split('\n')
        for linha in linhas[1:]:  # pula cabeçalho
            linha = linha.rstrip('\r\n')
            partes = linha.split()
            if len(partes) < 3:
                continue
            ultimo = partes[-1]
            segundo = partes[1]
            if segundo == ultimo:
                # Formato A: pos CODIGO descricao CODIGO
                codigo = segundo
                if '-' in codigo:
                    continue  # faixa agregada, pular
                resto = partes[2:-1]
            else:
                # Formato B: pos descricao CODIGO
                codigo = ultimo
                if '-' in codigo:
                    continue  # faixa agregada (ex: 00-99), pular
                resto = partes[1:-1]
            descricao = ' '.join(resto).strip()
            if not descricao:
                continue
            # Expandir múltiplos códigos (ex: "2,3" → dois registros com mesmo rótulo)
            for cod in codigo.split(','):
                cod = cod.strip()
                if cod:
                    registros.append({'codigo': cod, 'descricao': descricao})
    except Exception as e:
        logger.error(f"Erro CNV: {e}")
    return registros


def parse_dbf(conteudo: bytes) -> list:
    registros = []
    try:
        num_reg     = struct.unpack_from('<I', conteudo, 4)[0]
        header_size = struct.unpack_from('<H', conteudo, 8)[0]
        record_size = struct.unpack_from('<H', conteudo, 10)[0]
        campos, offset = [], 32
        while offset < header_size - 1 and conteudo[offset] != 0x0D:
            nome    = conteudo[offset:offset+11].split(b'\x00')[0].decode('latin-1')
            tamanho = conteudo[offset+16]
            campos.append({'nome': nome, 'tamanho': tamanho})
            offset += 32
        pos = header_size
        for _ in range(num_reg):
            if pos >= len(conteudo): break
            flag = conteudo[pos]; pos += 1
            if flag == 0x2A:
                pos += record_size - 1; continue
            r = {}
            for c in campos:
                r[c['nome']] = conteudo[pos:pos+c['tamanho']].decode('latin-1', errors='replace').strip()
                pos += c['tamanho']
            registros.append(r)
    except Exception as e:
        logger.error(f"Erro DBF: {e}")
    return registros


def salvar_csv(registros, caminho, col_cod, col_desc):
    if not registros:
        logger.warning(f"  Sem registros: {caminho}")
        return False
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow([col_cod, col_desc])
        for r in registros:
            keys = list(r.keys())
            if 'codigo' in r:
                w.writerow([r['codigo'], r.get('descricao', '')])
            elif len(keys) >= 2:
                w.writerow([r[keys[0]], r[keys[1]]])
    logger.info(f"  {caminho.name:<25s} {len(registros):>6,} registros")
    return True


def parse_cnv_cid_hierarquia(conteudo: bytes) -> list:
    """
    Parser para CID10CAP.CNV e CID10GRUPO.CNV.
    Formato de largura fixa: código CID (faixa) está no final da linha após padding.
    Ex: "      1  I.   Algumas doenças infecciosas e parasitárias    A00-B99,"
    O código é identificado pelo padrão [A-Z][0-9]+-[A-Z][0-9]+ no final da linha.
    """
    padrao_cid = re.compile(r'([A-Z]\d{2}-[A-Z]\d{2,3}),?\s*$')
    registros = []
    try:
        linhas = conteudo.decode('latin-1', errors='replace').strip().split('\n')
        for linha in linhas[1:]:  # pula cabeçalho
            linha = linha.rstrip('\r\n')
            m = padrao_cid.search(linha)
            if not m:
                continue
            codigo = m.group(1)
            sem_pos = linha.lstrip()
            sem_pos = re.sub(r'^\d+\s+', '', sem_pos)
            descricao = linha[:m.start()].strip()
            descricao = re.sub(r'^\s*\d+\s+', '', descricao).strip()
            if not descricao:
                continue
            registros.append({'codigo': codigo, 'descricao': descricao})
    except Exception as e:
        logger.error(f"Erro CNV hierarquia CID: {e}")
    return registros


def _cid_capitulo(cat3: str) -> str:
    """Retorna descrição do capítulo CID-10 a partir da categoria (3 chars)."""
    _CAPS = [
        ("A00","B99","I. Algumas doenças infecciosas e parasitárias"),
        ("C00","D48","II. Neoplasias [tumores]"),
        ("D50","D89","III. Doenças do sangue e dos órgãos hematopoéticos"),
        ("E00","E90","IV. Doenças endócrinas, nutricionais e metabólicas"),
        ("F00","F99","V. Transtornos mentais e comportamentais"),
        ("G00","G99","VI. Doenças do sistema nervoso"),
        ("H00","H59","VII. Doenças do olho e anexos"),
        ("H60","H95","VIII. Doenças do ouvido e da apófise mastóide"),
        ("I00","I99","IX. Doenças do aparelho circulatório"),
        ("J00","J99","X. Doenças do aparelho respiratório"),
        ("K00","K93","XI. Doenças do aparelho digestivo"),
        ("L00","L99","XII. Doenças da pele e do tecido subcutâneo"),
        ("M00","M99","XIII. Doenças do sistema osteomuscular e do tecido conjuntivo"),
        ("N00","N99","XIV. Doenças do aparelho geniturinário"),
        ("O00","O99","XV. Gravidez, parto e puerpério"),
        ("P00","P96","XVI. Algumas afecções originadas no período perinatal"),
        ("Q00","Q99","XVII. Malformações congênitas, deformidades e anomalias cromossômicas"),
        ("R00","R99","XVIII. Sintomas, sinais e achados anormais"),
        ("S00","T98","XIX. Lesões, envenenamento e outras consequências de causas externas"),
        ("V01","Y98","XX. Causas externas de morbidade e de mortalidade"),
        ("Z00","Z99","XXI. Fatores que influenciam o estado de saúde"),
        ("U00","U99","XXII. Códigos para propósitos especiais"),
    ]
    for ini, fim, desc in _CAPS:
        if ini <= cat3 <= fim:
            return desc
    return ""


def _cid_grupo(cat3: str) -> str:
    """Retorna descrição do grupo CID-10 a partir da categoria (3 chars)."""
    _GRPS = [
        ("A00","A09","A00-A09 Doenças infecciosas intestinais"),
        ("A15","A19","A15-A19 Tuberculose"),
        ("A20","A28","A20-A28 Algumas doenças bacterianas zoonóticas"),
        ("A30","A49","A30-A49 Outras doenças bacterianas"),
        ("A50","A64","A50-A64 Infecções de transmissão predominantemente sexual"),
        ("A65","A69","A65-A69 Outras doenças por espiroquetas"),
        ("A70","A74","A70-A74 Outras doenças causadas por clamídias"),
        ("A75","A79","A75-A79 Rickettsioses"),
        ("A80","A89","A80-A89 Infecções virais do sistema nervoso central"),
        ("A90","A99","A90-A99 Febres por arbovírus e febres hemorrágicas virais"),
        ("B00","B09","B00-B09 Infecções virais com lesões de pele e mucosas"),
        ("B15","B19","B15-B19 Hepatite viral"),
        ("B20","B24","B20-B24 Doença pelo vírus da imunodeficiência humana [HIV]"),
        ("B25","B34","B25-B34 Outras doenças por vírus"),
        ("B35","B49","B35-B49 Micoses"),
        ("B50","B64","B50-B64 Doenças devidas a protozoários"),
        ("B65","B83","B65-B83 Helmintíases"),
        ("B85","B89","B85-B89 Pediculose, acaríase e outras infestações"),
        ("B90","B94","B90-B94 Sequelas de doenças infecciosas e parasitárias"),
        ("B95","B97","B95-B97 Agentes de infecções bacterianas, virais e outros"),
        ("B99","B99","B99-B99 Outras doenças infecciosas"),
        ("C00","C75","C00-C75 Neoplasias malignas"),
        ("C76","C80","C76-C80 Neoplasias malignas de localizações mal definidas"),
        ("C81","C96","C81-C96 Neoplasias malignas do tecido linfático e hematopoético"),
        ("C97","C97","C97-C97 Neoplasias malignas de localizações múltiplas independentes"),
        ("D00","D09","D00-D09 Neoplasias in situ"),
        ("D10","D36","D10-D36 Neoplasias benignas"),
        ("D37","D48","D37-D48 Neoplasias de comportamento incerto ou desconhecido"),
        ("D50","D53","D50-D53 Anemias nutricionais"),
        ("D55","D59","D55-D59 Anemias hemolíticas"),
        ("D60","D64","D60-D64 Anemias aplásticas e outras anemias"),
        ("D65","D69","D65-D69 Defeitos da coagulação, púrpura e outras afecções hemorrágicas"),
        ("D70","D77","D70-D77 Outras doenças do sangue e dos órgãos hematopoéticos"),
        ("D80","D89","D80-D89 Alguns transtornos que comprometem o mecanismo imunitário"),
        ("E00","E07","E00-E07 Transtornos da glândula tireóide"),
        ("E10","E14","E10-E14 Diabetes mellitus"),
        ("E15","E16","E15-E16 Outros transtornos da regulação da glicose"),
        ("E20","E35","E20-E35 Transtornos de outras glândulas endócrinas"),
        ("E40","E46","E40-E46 Desnutrição"),
        ("E50","E64","E50-E64 Outras deficiências nutricionais"),
        ("E65","E68","E65-E68 Obesidade e outras formas de hiperalimentação"),
        ("E70","E90","E70-E90 Distúrbios metabólicos"),
        ("F00","F09","F00-F09 Transtornos mentais orgânicos"),
        ("F10","F19","F10-F19 Transtornos devidos ao uso de substância psicoativa"),
        ("F20","F29","F20-F29 Esquizofrenia e transtornos delirantes"),
        ("F30","F39","F30-F39 Transtornos do humor [afetivos]"),
        ("F40","F48","F40-F48 Transtornos neuróticos e somatoformes"),
        ("F50","F59","F50-F59 Síndromes comportamentais associadas a disfunções fisiológicas"),
        ("F60","F69","F60-F69 Transtornos da personalidade e do comportamento do adulto"),
        ("F70","F79","F70-F79 Retardo mental"),
        ("F80","F89","F80-F89 Transtornos do desenvolvimento psicológico"),
        ("F90","F98","F90-F98 Transtornos do comportamento e emocionais"),
        ("F99","F99","F99-F99 Transtorno mental não especificado"),
        ("G00","G09","G00-G09 Doenças inflamatórias do sistema nervoso central"),
        ("G10","G13","G10-G13 Atrofias sistêmicas do sistema nervoso central"),
        ("G20","G26","G20-G26 Doenças extrapiramidais e transtornos dos movimentos"),
        ("G30","G32","G30-G32 Outras doenças degenerativas do sistema nervoso"),
        ("G35","G37","G35-G37 Doenças desmielinizantes do sistema nervoso central"),
        ("G40","G47","G40-G47 Transtornos episódicos e paroxísticos"),
        ("G50","G59","G50-G59 Transtornos dos nervos, raízes e plexos nervosos"),
        ("G60","G64","G60-G64 Polineuropatias e outros transtornos do SNP"),
        ("G70","G73","G70-G73 Doenças da junção mioneural e dos músculos"),
        ("G80","G83","G80-G83 Paralisia cerebral e outras síndromes paralíticas"),
        ("G90","G99","G90-G99 Outros transtornos do sistema nervoso"),
        ("H00","H06","H00-H06 Transtornos da pálpebra, aparelho lacrimal e órbita"),
        ("H10","H13","H10-H13 Transtornos da conjuntiva"),
        ("H15","H22","H15-H22 Transtornos da esclerótica, córnea, íris e corpo ciliar"),
        ("H25","H28","H25-H28 Transtornos do cristalino"),
        ("H30","H36","H30-H36 Transtornos da coróide e da retina"),
        ("H40","H42","H40-H42 Glaucoma"),
        ("H43","H45","H43-H45 Transtornos do humor vítreo e do globo ocular"),
        ("H46","H48","H46-H48 Transtornos do nervo óptico e das vias ópticas"),
        ("H49","H52","H49-H52 Transtornos dos músculos oculares e da refração"),
        ("H53","H54","H53-H54 Transtornos visuais e cegueira"),
        ("H55","H59","H55-H59 Outros transtornos do olho e anexos"),
        ("H60","H62","H60-H62 Doenças do ouvido externo"),
        ("H65","H75","H65-H75 Doenças do ouvido médio e da mastóide"),
        ("H80","H83","H80-H83 Doenças do ouvido interno"),
        ("H90","H95","H90-H95 Outros transtornos do ouvido"),
        ("I00","I02","I00-I02 Febre reumática aguda"),
        ("I05","I09","I05-I09 Doenças reumáticas crônicas do coração"),
        ("I10","I15","I10-I15 Doenças hipertensivas"),
        ("I20","I25","I20-I25 Doenças isquêmicas do coração"),
        ("I26","I28","I26-I28 Doença cardíaca pulmonar"),
        ("I30","I52","I30-I52 Outras formas de doença do coração"),
        ("I60","I69","I60-I69 Doenças cerebrovasculares"),
        ("I70","I79","I70-I79 Doenças das artérias, arteríolas e capilares"),
        ("I80","I89","I80-I89 Doenças das veias e dos vasos linfáticos"),
        ("I95","I99","I95-I99 Outros transtornos do aparelho circulatório"),
        ("J00","J06","J00-J06 Infecções agudas das vias aéreas superiores"),
        ("J09","J18","J09-J18 Influenza [gripe] e pneumonia"),
        ("J20","J22","J20-J22 Outras infecções agudas das vias aéreas inferiores"),
        ("J30","J39","J30-J39 Outras doenças das vias aéreas superiores"),
        ("J40","J47","J40-J47 Doenças crônicas das vias aéreas inferiores"),
        ("J60","J70","J60-J70 Doenças pulmonares devidas a agentes externos"),
        ("J80","J84","J80-J84 Outras doenças respiratórias do interstício"),
        ("J85","J86","J85-J86 Afecções supurativas das vias aéreas inferiores"),
        ("J90","J94","J90-J94 Outras doenças da pleura"),
        ("J95","J99","J95-J99 Outras doenças do aparelho respiratório"),
        ("K00","K14","K00-K14 Doenças da cavidade oral, glândulas salivares e maxilares"),
        ("K20","K31","K20-K31 Doenças do esôfago, estômago e duodeno"),
        ("K35","K38","K35-K38 Doenças do apêndice"),
        ("K40","K46","K40-K46 Hérnias"),
        ("K50","K52","K50-K52 Enterites e colites não-infecciosas"),
        ("K55","K63","K55-K63 Outras doenças dos intestinos"),
        ("K65","K67","K65-K67 Doenças do peritônio"),
        ("K70","K77","K70-K77 Doenças do fígado"),
        ("K80","K87","K80-K87 Transtornos da vesícula biliar, vias biliares e pâncreas"),
        ("K90","K93","K90-K93 Outras doenças do aparelho digestivo"),
        ("L00","L08","L00-L08 Infecções da pele e do tecido subcutâneo"),
        ("L10","L14","L10-L14 Afecções bolhosas"),
        ("L20","L30","L20-L30 Dermatite e eczema"),
        ("L40","L45","L40-L45 Afecções pápulo-descamativas"),
        ("L50","L54","L50-L54 Urticária e eritema"),
        ("L55","L59","L55-L59 Transtornos relacionados com a radiação"),
        ("L60","L75","L60-L75 Afecções dos anexos da pele"),
        ("L80","L99","L80-L99 Outras afecções da pele e do tecido subcutâneo"),
        ("M00","M25","M00-M25 Artropatias"),
        ("M30","M36","M30-M36 Afecções sistêmicas do tecido conjuntivo"),
        ("M40","M54","M40-M54 Dorsopatias"),
        ("M60","M79","M60-M79 Transtornos dos tecidos moles"),
        ("M80","M94","M80-M94 Osteopatias e condropatias"),
        ("M95","M99","M95-M99 Outros transtornos do sistema osteomuscular"),
        ("N00","N08","N00-N08 Doenças glomerulares"),
        ("N10","N16","N10-N16 Doenças renais túbulo-intersticiais"),
        ("N17","N19","N17-N19 Insuficiência renal"),
        ("N20","N23","N20-N23 Calculose renal"),
        ("N25","N29","N25-N29 Outros transtornos do rim e do ureter"),
        ("N30","N39","N30-N39 Outras doenças do aparelho urinário"),
        ("N40","N51","N40-N51 Doenças dos órgãos genitais masculinos"),
        ("N60","N64","N60-N64 Transtornos da mama"),
        ("N70","N77","N70-N77 Doenças inflamatórias dos órgãos pélvicos femininos"),
        ("N80","N98","N80-N98 Transtornos não-inflamatórios do trato genital feminino"),
        ("N99","N99","N99-N99 Outros transtornos do aparelho geniturinário"),
        ("O00","O08","O00-O08 Gravidez que termina em aborto"),
        ("O10","O16","O10-O16 Edema, proteinúria e transtornos hipertensivos na gravidez"),
        ("O20","O29","O20-O29 Outros transtornos maternos relacionados com a gravidez"),
        ("O30","O48","O30-O48 Assistência à mãe por motivos ligados ao feto"),
        ("O60","O75","O60-O75 Complicações do trabalho de parto e do parto"),
        ("O80","O84","O80-O84 Parto"),
        ("O85","O92","O85-O92 Complicações relacionadas com o puerpério"),
        ("O94","O99","O94-O99 Outras afecções obstétricas NCOP"),
        ("P00","P04","P00-P04 Feto e recém-nascido afetados por fatores maternos"),
        ("P05","P08","P05-P08 Transtornos da duração da gestação e crescimento fetal"),
        ("P10","P15","P10-P15 Traumatismo de parto"),
        ("P20","P29","P20-P29 Transtornos respiratórios e cardiovasculares perinatais"),
        ("P35","P39","P35-P39 Infecções específicas do período perinatal"),
        ("P50","P61","P50-P61 Transtornos hemorrágicos e hematológicos do feto/RN"),
        ("P70","P74","P70-P74 Transtornos endócrinos e metabólicos transitórios do feto/RN"),
        ("P75","P78","P75-P78 Transtornos do aparelho digestivo do feto/RN"),
        ("P80","P83","P80-P83 Afecções do tegumento e regulação térmica do feto/RN"),
        ("P90","P96","P90-P96 Outros transtornos originados no período perinatal"),
        ("Q00","Q07","Q00-Q07 Malformações congênitas do sistema nervoso"),
        ("Q10","Q18","Q10-Q18 Malformações congênitas do olho, ouvido, face e pescoço"),
        ("Q20","Q28","Q20-Q28 Malformações congênitas do aparelho circulatório"),
        ("Q30","Q34","Q30-Q34 Malformações congênitas do aparelho respiratório"),
        ("Q35","Q37","Q35-Q37 Fenda labial e fenda palatina"),
        ("Q38","Q45","Q38-Q45 Outras malformações congênitas do aparelho digestivo"),
        ("Q50","Q56","Q50-Q56 Malformações congênitas dos órgãos genitais"),
        ("Q60","Q64","Q60-Q64 Malformações congênitas do aparelho urinário"),
        ("Q65","Q79","Q65-Q79 Malformações e deformidades congênitas do sistema osteomuscular"),
        ("Q80","Q89","Q80-Q89 Outras malformações congênitas"),
        ("Q90","Q99","Q90-Q99 Anomalias cromossômicas NCOP"),
        ("R00","R09","R00-R09 Sintomas relativos ao aparelho circulatório e respiratório"),
        ("R10","R19","R10-R19 Sintomas relativos ao aparelho digestivo e ao abdome"),
        ("R20","R23","R20-R23 Sintomas relativos à pele e ao tecido subcutâneo"),
        ("R25","R29","R25-R29 Sintomas relativos aos sistemas nervoso e osteomuscular"),
        ("R30","R39","R30-R39 Sintomas relativos ao aparelho urinário"),
        ("R40","R46","R40-R46 Sintomas relativos à cognição, percepção e comportamento"),
        ("R47","R49","R47-R49 Sintomas relativos à fala e à voz"),
        ("R50","R69","R50-R69 Sintomas e sinais gerais"),
        ("R70","R79","R70-R79 Achados anormais de exames de sangue"),
        ("R80","R82","R80-R82 Achados anormais de exames de urina"),
        ("R83","R89","R83-R89 Achados anormais de exames de outros líquidos e tecidos"),
        ("R90","R94","R90-R94 Achados anormais de exames por imagem e estudos de função"),
        ("R95","R99","R95-R99 Causas mal definidas e desconhecidas de mortalidade"),
        ("S00","S09","S00-S09 Traumatismos da cabeça"),
        ("S10","S19","S10-S19 Traumatismos do pescoço"),
        ("S20","S29","S20-S29 Traumatismos do tórax"),
        ("S30","S39","S30-S39 Traumatismos do abdome, dorso, coluna lombar e pelve"),
        ("S40","S49","S40-S49 Traumatismos do ombro e do braço"),
        ("S50","S59","S50-S59 Traumatismos do cotovelo e do antebraço"),
        ("S60","S69","S60-S69 Traumatismos do punho e da mão"),
        ("S70","S79","S70-S79 Traumatismos do quadril e da coxa"),
        ("S80","S89","S80-S89 Traumatismos do joelho e da perna"),
        ("S90","S99","S90-S99 Traumatismos do tornozelo e do pé"),
        ("T00","T07","T00-T07 Traumatismos envolvendo múltiplas regiões do corpo"),
        ("T08","T14","T08-T14 Traumatismos de localização não especificada"),
        ("T15","T19","T15-T19 Efeitos de corpo estranho por orifício natural"),
        ("T20","T32","T20-T32 Queimaduras e corrosões"),
        ("T33","T35","T33-T35 Geladuras [frostbite]"),
        ("T36","T50","T36-T50 Intoxicação por drogas e substâncias biológicas"),
        ("T51","T65","T51-T65 Efeitos tóxicos de substâncias não-medicinais"),
        ("T66","T78","T66-T78 Outros efeitos de causas externas"),
        ("T79","T79","T79-T79 Algumas complicações precoces de traumatismos"),
        ("T80","T88","T80-T88 Complicações de cuidados médicos e cirúrgicos NCOP"),
        ("T90","T98","T90-T98 Sequelas de traumatismos e intoxicações"),
        ("V01","V09","V01-V09 Pedestre traumatizado em acidente de transporte"),
        ("V10","V19","V10-V19 Ciclista traumatizado em acidente de transporte"),
        ("V20","V29","V20-V29 Motociclista traumatizado em acidente de transporte"),
        ("V30","V39","V30-V39 Ocupante de triciclo motorizado traumatizado"),
        ("V40","V49","V40-V49 Ocupante de automóvel traumatizado"),
        ("V50","V59","V50-V59 Ocupante de caminhonete traumatizado"),
        ("V60","V69","V60-V69 Ocupante de veículo pesado traumatizado"),
        ("V70","V79","V70-V79 Ocupante de ônibus traumatizado"),
        ("V80","V89","V80-V89 Outros acidentes de transporte terrestre"),
        ("V90","V94","V90-V94 Acidentes de transporte por água"),
        ("V95","V97","V95-V97 Acidentes de transporte aéreo e espacial"),
        ("V98","V99","V98-V99 Outros acidentes de transporte"),
        ("W00","X59","W00-X59 Outras causas externas de traumatismos acidentais"),
        ("X60","X84","X60-X84 Lesões autoprovocadas voluntariamente"),
        ("X85","Y09","X85-Y09 Agressões"),
        ("Y10","Y34","Y10-Y34 Eventos cuja intenção é indeterminada"),
        ("Y35","Y36","Y35-Y36 Intervenções legais e operações de guerra"),
        ("Y40","Y84","Y40-Y84 Complicações de assistência médica e cirúrgica"),
        ("Y85","Y89","Y85-Y89 Sequelas de causas externas"),
        ("Y90","Y98","Y90-Y98 Fatores suplementares relacionados com morbidade e mortalidade"),
        ("Z00","Z13","Z00-Z13 Pessoas em contato com serviços de saúde para exame"),
        ("Z20","Z29","Z20-Z29 Pessoas com riscos relacionados com doenças transmissíveis"),
        ("Z30","Z39","Z30-Z39 Pessoas em contato com serviços de saúde — reprodução"),
        ("Z40","Z54","Z40-Z54 Pessoas em contato com serviços de saúde — procedimentos"),
        ("Z55","Z65","Z55-Z65 Pessoas com riscos socioeconômicos e psicossociais"),
        ("Z70","Z76","Z70-Z76 Pessoas em contato com serviços de saúde — outras"),
        ("Z80","Z99","Z80-Z99 Pessoas com riscos relacionados com história pessoal e familiar"),
        ("U00","U49","U00-U49 Designação provisória de novas doenças"),
        ("U50","U99","U50-U99 Resistência a drogas antimicrobianas"),
    ]
    for ini, fim, desc in _GRPS:
        if ini <= cat3 <= fim:
            return desc
    return ""


_CID_VALIDO = re.compile(r'^[A-Z]\d{2,3}$')

# Prefixo CID grudado no CD_DESCR do DBF (ex: "A00  Colera", "A00.0 Colera dev...")
_CID_PREFIX_DESC = re.compile(r'^[A-Z]\d{2}\.?\d?\s+')

# LEGADO — Categorias ausentes no DBF do DATASUS (códigos mais recentes da CID-10 OMS).
# Sem esse fallback, DS_CATEGORIA fica vazia para esses códigos.
# _CID_CATEGORIAS_MANUAIS = {
#     "U04": "Síndrome respiratória aguda grave",
#     "U07": "COVID-19",
#     "U09": "Condição pós-COVID-19",
#     "U10": "Síndrome inflamatória multissistêmica associada a COVID-19",
#     "N18": "Doença renal crônica",
#     "U80": "Agente resistente a penicilina e antibióticos relacionados",
#     "U81": "Agente resistente a vancomicina e antibióticos relacionados",
#     "U82": "Resistência a antibióticos beta-lactâmicos",
#     "U83": "Resistência a outros antibióticos",
#     "U89": "Agente resistente a outros agentes antimicrobianos",
#     "W46": "Contato com agulha hipodérmica",
# }

# LEGADO — Registros CID completos ausentes no S_CID.DBF — injetados no cid.csv após extração.
# Origem: auditoria empírica check_quality.py (2026-03-07) — 12 códigos órfãos em
# 183.877.219 internações. Todos são códigos OMS adicionados após a versão do DBF
# disponível no FTP DATASUS.
# Fonte: OMS CID-10 v2010 (N18x), v2003 (U04), v2020-2021 (U07/U09/U10/U099/U109),
#        v2010 (U80-U89).
# Formato: CID, DESCRICAO, TP_NIVEL, RESTRSEXO, DS_CATEGORIA, DS_GRUPO, DS_CAPITULO
# _CID_MANUAIS_COMPLETOS = [
#     # Capítulo XXII — Códigos para propósitos especiais
#     ("U04",  "Síndrome respiratória aguda grave [SARS]",                          "SUBCAT", "",
#      "Síndrome respiratória aguda grave",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     ("U07",  "COVID-19",                                                           "SUBCAT", "",
#      "COVID-19",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     ("U09",  "Condição pós-COVID-19",                                             "SUBCAT", "",
#      "Condição pós-COVID-19",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     ("U099", "Condição pós-COVID-19, não especificada",                           "SUBCAT", "",
#      "Condição pós-COVID-19",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     ("U10",  "Síndrome inflamatória multissistêmica associada a COVID-19",        "SUBCAT", "",
#      "Síndrome inflamatória multissistêmica associada a COVID-19",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     ("U109", "Síndrome inflamatória multissistêmica assoc. COVID-19, n.e.",       "SUBCAT", "",
#      "Síndrome inflamatória multissistêmica associada a COVID-19",
#      "U00-U49 Designação provisória de novas doenças",
#      "XXII. Códigos para propósitos especiais"),
#     # Capítulo XIV — Aparelho geniturinário (subcategorias N18 adicionadas CID-10 v2010)
#     ("N182", "Doença renal crônica, estádio 2",                                   "SUBCAT", "F",
#      "Doença renal crônica",
#      "N17-N19 Insuficiência renal",
#      "XIV. Doenças do aparelho geniturinário"),
#     ("N183", "Doença renal crônica, estádio 3",                                   "SUBCAT", "F",
#      "Doença renal crônica",
#      "N17-N19 Insuficiência renal",
#      "XIV. Doenças do aparelho geniturinário"),
#     ("N184", "Doença renal crônica, estádio 4",                                   "SUBCAT", "F",
#      "Doença renal crônica",
#      "N17-N19 Insuficiência renal",
#      "XIV. Doenças do aparelho geniturinário"),
#     ("N185", "Doença renal crônica, estádio 5",                                   "SUBCAT", "F",
#      "Doença renal crônica",
#      "N17-N19 Insuficiência renal",
#      "XIV. Doenças do aparelho geniturinário"),
#     # Capítulo XXII — Resistência antimicrobiana (CID-10 v2010)
#     ("U80",  "Agente resistente a penicilina e antibióticos relacionados",        "SUBCAT", "",
#      "Agente resistente a penicilina e antibióticos relacionados",
#      "U50-U99 Resistência a drogas antimicrobianas",
#      "XXII. Códigos para propósitos especiais"),
#     ("U81",  "Agente resistente a vancomicina e antibióticos relacionados",       "SUBCAT", "",
#      "Agente resistente a vancomicina e antibióticos relacionados",
#      "U50-U99 Resistência a drogas antimicrobianas",
#      "XXII. Códigos para propósitos especiais"),
#     ("U89",  "Resistência a outros agentes antimicrobianos",                      "SUBCAT", "",
#      "Resistência a outros agentes antimicrobianos",
#      "U50-U99 Resistência a drogas antimicrobianas",
#      "XXII. Códigos para propósitos especiais"),
# ]


def extrair_scid(registros_dbf: list, caminho: Path) -> bool:
    """S_CID.DBF — extrai CID-10 para TD_CID com hierarquia derivada.
    Colunas: CID, DESCRICAO, TP_NIVEL, RESTRSEXO, DS_CATEGORIA, DS_GRUPO, DS_CAPITULO.
    Hierarquia: Capítulo > Grupo > Categoria > Subcategoria (4 níveis CID-10 OMS).

    Filtros aplicados (3 camadas):
      1. Regex: código deve ser 1 letra maiúscula + 2-3 dígitos (remove j450, E.78, ZZZ, M81*, etc.)
      2. Descrição: remove "CID NÇO IDENTIFICADO" (erros de digitação hospitalares)
      3. DS_CATEGORIA: preenche códigos recentes ausentes no DBF via _CID_CATEGORIAS_MANUAIS
    """
    if not registros_dbf:
        return False

    # Pré-montar lookup de categorias (3 chars → descrição)
    cat_desc = {}
    for r in registros_dbf:
        if r.get('CAT') == 'S':
            cod = r.get('CD_COD', '').strip()
            desc = r.get('CD_DESCR', '').strip()
            # FIX: CD_DESCR vem com código grudado (ex: "A00  Colera")
            desc = _CID_PREFIX_DESC.sub('', desc)
            if len(cod) == 3:
                cat_desc[cod] = desc

    # LEGADO — Fallback manual para categorias ausentes no DBF
    # for cat3, desc in _CID_CATEGORIAS_MANUAIS.items():
    #     if cat3 not in cat_desc:
    #         cat_desc[cat3] = desc

    n_regex = 0
    n_nci = 0
    n_validos = 0
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['CID', 'DESCRICAO', 'TP_NIVEL', 'RESTRSEXO',
                     'DS_CATEGORIA', 'DS_GRUPO', 'DS_CAPITULO'])
        for r in registros_dbf:
            nivel = 'CAT' if r.get('CAT') == 'S' else 'SUBCAT'
            cod = r.get('CD_COD', '').strip()
            desc = r.get('CD_DESCR', '').strip()
            # FIX: CD_DESCR vem com código grudado (ex: "A00.0 Colera dev...")
            desc = _CID_PREFIX_DESC.sub('', desc)

            # Filtro 1: regex — só aceita [A-Z] + 2-3 dígitos
            if not _CID_VALIDO.match(cod):
                n_regex += 1
                continue

            # Filtro 2: remove erros de digitação hospitalares
            if 'NÇO IDENTIFICADO' in desc or 'NAO IDENTIFICADO' in desc:
                n_nci += 1
                continue

            cat3 = cod[:3]
            n_validos += 1
            w.writerow([
                cod,
                desc,
                nivel,
                r.get('RESTRSEXO', '').strip(),
                cat_desc.get(cat3, ''),
                _cid_grupo(cat3),
                _cid_capitulo(cat3),
            ])

    # Injetar códigos ausentes no DBF (auditoria empírica 2026-03-07)
    # 12 códigos OMS adicionados após versão do DBF disponível no FTP DATASUS.
    n_injetados = 0
   # codigos_existentes = set()
   # with open(caminho, 'r', encoding='utf-8') as f:
   #     import csv as _csv
   #     for row in _csv.DictReader(f):
   #         codigos_existentes.add(row.get('CID', '').strip())

    #with open(caminho, 'a', encoding='utf-8', newline='') as f:
    #    w = csv.writer(f)
    #    for row in _CID_MANUAIS_COMPLETOS:
    #        if row[0] not in codigos_existentes:
    #            w.writerow(row)
    #            n_injetados += 1

    logger.info(f"  {caminho.name:<25s} {n_validos:>6,} registros "
               f"(-{n_regex} malformado, -{n_nci} NÇO IDENT., +{n_injetados} manuais OMS)")
    return True


def extrair_cadhosp(registros_dbf: list, caminho: Path) -> bool:
    """CADHOSP.DBF — extrai CGC_HOSP, RAZAO, UF_ZI, CMPT para TD_HOSPITAL."""
    if not registros_dbf:
        return False
    colunas_disponiveis = list(registros_dbf[0].keys())
    logger.info(f"  Colunas CADHOSP: {colunas_disponiveis}")
    col_cgc  = next((c for c in colunas_disponiveis if 'CGC'  in c.upper()), None)
    col_razao= next((c for c in colunas_disponiveis if 'RAZAO' in c.upper() or 'NOME' in c.upper()), None)
    col_uf   = next((c for c in colunas_disponiveis if 'UF' in c.upper()), None)
    col_cmpt = next((c for c in colunas_disponiveis if 'CMPT' in c.upper()), None)
    colunas_out = [c for c in [col_cgc, col_razao, col_uf, col_cmpt] if c]
    if not colunas_out:
        logger.warning("  CADHOSP: nenhuma coluna esperada encontrada")
        return False

    # Filtrar registros sem CGC (placeholders)
    n_antes = len(registros_dbf)
    if col_cgc:
        registros_dbf = [r for r in registros_dbf if r.get(col_cgc, '').strip()]
    n_filtrados = n_antes - len(registros_dbf)

    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(colunas_out)
        for r in registros_dbf:
            w.writerow([r.get(c, '') for c in colunas_out])
    logger.info(f"  {caminho.name:<25s} {len(registros_dbf):>6,} registros (-{n_filtrados} sem CGC)")
    return True

def extrair_tcnes(registros_dbf: list, caminho: Path) -> bool:
    """TCNESBR.dbf — extrai CNES, NOMEFANT para TD_HOSPITAL (join direto por CNES)."""
    if not registros_dbf:
        return False
    colunas_disponiveis = list(registros_dbf[0].keys())
    col_cnes = next((c for c in colunas_disponiveis if 'CNES' in c.upper()), None)
    col_nome = next((c for c in colunas_disponiveis if 'NOMEFANT' in c.upper()), None)
    colunas_out = [c for c in [col_cnes, col_nome] if c]
    if not colunas_out:
        logger.warning("  TCNESBR: nenhuma coluna esperada encontrada")
        return False

    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(colunas_out)
        for r in registros_dbf:
            w.writerow([r.get(c, '') for c in colunas_out])
    logger.info(f"  {caminho.name:<25s} {len(registros_dbf):>6,} registros")
    return True


def extrair_sigtap(registros_dbf: list, caminho: Path) -> bool:
    """TB_SIGTAP.dbf — extrai código e descrição do procedimento para TD_PROCEDIMENTO."""
    if not registros_dbf:
        return False
    colunas_disponiveis = list(registros_dbf[0].keys())
    logger.info(f"  Colunas TB_SIGTAP: {colunas_disponiveis}")
    col_cod  = next((c for c in colunas_disponiveis if 'CO_' in c.upper() or c.upper() in ('CO_PROCED', 'CODIGO', 'CO_PROC')), None)
    col_desc = next((c for c in colunas_disponiveis if 'DS_' in c.upper() or 'DESCRI' in c.upper() or 'NOME' in c.upper()), None)
    if not col_cod or not col_desc:
        col_cod, col_desc = colunas_disponiveis[0], colunas_disponiveis[1]
        logger.warning(f"  TB_SIGTAP: usando fallback — {col_cod}, {col_desc}")
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(['PROC_REA', 'NOME_PROC'])
        for r in registros_dbf:
            w.writerow([r.get(col_cod, ''), r.get(col_desc, '')])
    logger.info(f"  {caminho.name:<25s} {len(registros_dbf):>6,} registros")
    return True


def extrair_cid10(registros_dbf: list, caminho: Path) -> bool:
    """cid10.dbf — extrai código e descrição CID-10 para TD_CID."""
    if not registros_dbf:
        return False
    colunas_disponiveis = list(registros_dbf[0].keys())
    logger.info(f"  Colunas cid10: {colunas_disponiveis}")
    with open(caminho, 'w', newline='', encoding='utf-8') as f:
        w = csv.writer(f)
        w.writerow(colunas_disponiveis)
        for r in registros_dbf:
            w.writerow([r.get(c, '') for c in colunas_disponiveis])
    logger.info(f"  {caminho.name:<25s} {len(registros_dbf):>6,} registros")
    return True


def processar_especiais(z: zipfile.ZipFile, arquivos_zip: list, saida_dir: Path) -> tuple:
    """Processa tabelas com lógica de extração especial."""
    sucessos, falhas = [], []

    for campo, csv_nome in TABELAS_ESPECIAIS.items():
        candidatos = [
            a for a in arquivos_zip
            if Path(a).stem.upper() == campo.upper()
            and Path(a).suffix.lower() in ['.cnv', '.dbf']
        ]

        # br_regsaud: pegar o arquivo nacional (br_regsaud.cnv ou br_regsaudc.cnv)
        if not candidatos and campo == 'br_regsaud':
            candidatos = [
                a for a in arquivos_zip
                if Path(a).stem.lower().startswith('br_regsaud')
                and Path(a).suffix.lower() == '.cnv'
                and 'mun' not in Path(a).stem.lower()
                and not Path(a).stem.lower().endswith('n')
            ]

        if not candidatos:
            logger.warning(f"  {campo} — não encontrado no ZIP")
            falhas.append(campo)
            continue

        arquivo_zip = candidatos[0]
        logger.info(f"\n--- {campo} → {arquivo_zip} ---")
        conteudo = z.read(arquivo_zip)
        ext = Path(arquivo_zip).suffix.lower()
        caminho_saida = saida_dir / csv_nome

        if ext == '.cnv':
            if campo.upper() in ('CID10CAP', 'CID10GRUPO'):
                registros = parse_cnv_cid_hierarquia(conteudo)
            else:
                registros = parse_cnv(conteudo)

            if campo.lower() == 'etnia':
                ok = salvar_csv(registros, caminho_saida, 'ETNIA', 'DESCRICAO')
            elif campo == 'br_regsaud':
                # FIX: parse_cnv gruda código da região no nome
                # ex: "43025 Região 25 - Vinhedo" → "Região 25 - Vinhedo"
                for r in registros:
                    partes = r['descricao'].split(None, 1)
                    if len(partes) == 2 and partes[0].isdigit():
                        r['descricao'] = partes[1]
                ok = salvar_csv(registros, caminho_saida, 'CODIGO', 'DESCRICAO')
            else:
                ok = salvar_csv(registros, caminho_saida, 'CODIGO', 'DESCRICAO')
        else:
            registros = parse_dbf(conteudo)
            if campo.upper() == 'S_CID':
                ok = extrair_scid(registros, caminho_saida)
            elif campo.upper() == 'CADHOSP':
                ok = extrair_cadhosp(registros, caminho_saida)
            elif campo.upper() == 'TB_SIGTAP':
                ok = extrair_sigtap(registros, caminho_saida)
            elif campo.lower() == 'cid10':
                ok = extrair_cid10(registros, caminho_saida)
            else:
                ok = salvar_csv(registros, caminho_saida, 'CODIGO', 'DESCRICAO')

        (sucessos if ok else falhas).append(campo)

    return sucessos, falhas


# LEGADO — _SENTINELAS garante valor "Não informado" em todas as dimensões.
# O preprocess.py faz fill_null(0) em campos Int8/Int16. Se o código 0 (ou 00,
# 99, 0000) não existir no CSV da dimensão, o load.py gera FK violation.
# Esta função insere o sentinela faltante após a geração de cada CSV.
# ---------------------------------------------------------------------------

# _SENTINELAS = {
#     # csv_nome:          (col_codigo, valor_sentinela, descrição)
#     'sexo.csv':          ('SEXO',          '0',    'Ignorado'),
#     'raca_cor.csv':      ('RACA_COR',      '0',    'Sem informação'),  # 99→0 após preprocess (bug .clip corrigido 2026-03)
#     'instrucao.csv':     ('INSTRU',        '0',    'Não informado'),
#     'car_int.csv':       ('CAR_INT',       '00',   'Não informado'),
#     'vincprev.csv':      ('VINCPREV',      '0',    'Não informado'),
#     'complexidade.csv':  ('COMPLEX',       '00',   'Não informado'),
#     'especialidade.csv': ('ESPEC',         '00',   'Não informado'),
#     'nacionalidade.csv': ('NACIONAL',      '0',    'Não informado'),
#     'contraceptivos.csv':('CONTRACEPTIVO', '00',   'Não informado'),
#     'etnia.csv':         ('ETNIA',         '0000', 'Não informado'),
#     # marca_uti.csv já tem 00=Não utilizou UTI no CNV
# }

# # instrucao.csv também precisa do sentinela 9=Ignorado (aparece nos microdados)
# _SENTINELAS_EXTRA = {
#     'instrucao.csv': [('INSTRU', '9', 'Ignorado')],
# }


# def _inserir_sentinelas(saida_dir: Path):
#     """Insere sentinelas faltantes nos CSVs de dimensão."""
#     n_inseridos = 0
#     for csv_nome, (col_cod, valor, descricao) in _SENTINELAS.items():
#         if _add_sentinel(saida_dir / csv_nome, col_cod, valor, descricao):
#             n_inseridos += 1
#
#     for csv_nome, lista in _SENTINELAS_EXTRA.items():
#         for col_cod, valor, descricao in lista:
#             if _add_sentinel(saida_dir / csv_nome, col_cod, valor, descricao):
#                 n_inseridos += 1
#
#     if n_inseridos:
#         logger.info(f"  Sentinelas inseridos: {n_inseridos}")


# def _add_sentinel(caminho: Path, col_cod: str, valor: str, descricao: str) -> bool:
#     """Adiciona sentinela se o valor ainda não existir no CSV. Retorna True se inseriu."""
#     if not caminho.exists():
#         return False
#
#     with open(caminho, 'r', encoding='utf-8', newline='') as f:
#         content = f.read()
#
#     # Normalizar newlines
#     content = content.replace('\r\n', '\n').replace('\r', '')
#     lines = content.strip().split('\n')
#     if len(lines) < 2:
#         return False
#
#     header = lines[0]
#
#     # Checar se valor já existe (primeira coluna)
#     reader = csv.reader(io.StringIO('\n'.join(lines[1:])))
#     codigos = {row[0].strip() for row in reader if row}
#
#     if valor in codigos:
#         return False
#
#     # Inserir sentinela
#     lines.insert(1, f"{valor},{descricao}")
#     with open(caminho, 'w', newline='', encoding='utf-8') as f:
#         f.write('\n'.join(lines) + '\n')
#
#     return True


def processar_zip(conteudo_zip: bytes, saida_dir: Path):
    with zipfile.ZipFile(BytesIO(conteudo_zip)) as z:
        arquivos_zip = z.namelist()
        logger.info(f"  ZIP: {len(arquivos_zip)} arquivos")
        logger.info(f"  Destino: {saida_dir}\n")

        sucessos, falhas = [], []

        # --- Tabelas padrão ---
        for campo, (csv_nome, col_cod, col_desc) in TABELAS_ALVO.items():
            candidatos = [
                a for a in arquivos_zip
                if Path(a).stem.upper() == campo.upper()
                and Path(a).suffix.lower() in ['.cnv', '.dbf']
            ]
            if not candidatos:
                # ESPEC não existe no TAB_SIH.zip — tratado pelo FIX 1 abaixo
                if campo == 'ESPEC':
                    logger.debug(f"  {campo} — não encontrado no ZIP (esperado, será baixado do CNES)")
                else:
                    logger.warning(f"  ERRO {campo} — não encontrado no ZIP")
                falhas.append(campo)
                continue

            arquivo_zip = candidatos[0]
            conteudo = z.read(arquivo_zip)
            ext = Path(arquivo_zip).suffix.lower()
            registros = parse_cnv(conteudo) if ext == '.cnv' else parse_dbf(conteudo)
            ok = salvar_csv(registros, saida_dir / csv_nome, col_cod, col_desc)
            (sucessos if ok else falhas).append(campo)

        # --- Tabelas especiais ---
        suc_esp, fal_esp = processar_especiais(z, arquivos_zip, saida_dir)
        sucessos += suc_esp
        falhas   += fal_esp

    # --- FIX 1: Fallback ESPEC (não existe no TAB_SIH.zip) ---
    if 'ESPEC' in falhas:
        csv_espec = saida_dir / TABELAS_ALVO['ESPEC'][0]
        if _gerar_especialidade_fallback(csv_espec):
            falhas.remove('ESPEC')
            sucessos.append('ESPEC')

    # --- Sentinelas ---
    #_inserir_sentinelas(saida_dir)

    # --- Resumo ---
    logger.info(f"\n  Resultado: {len(sucessos)} ok, {len(falhas)} falhas")
    if falhas:
        logger.warning(f"  Falhas: {falhas}")


# ---------------------------------------------------------------------------
# Municípios — cruzamento DATASUS + IBGE (fontes oficiais)
#
# Estratégia:
#   1. br_municip.cnv (TAB_SIH.zip, já baixado) → códigos 6d usados no SIH
#   2. API IBGE Localidades                      → códigos 7d, nomes, UFs
#   3. API IBGE Malhas (v3/v2 fallback)       → centroides oficiais
#   Cruzamento por codigo_6d = codigo_ibge // 10
# ---------------------------------------------------------------------------

_URL_IBGE_LOCALIDADES = (
    "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
)
_URLS_IBGE_MALHAS_UF = [
    # v3 — parâmetro correto para subdivisão municipal
    (
        "https://servicodados.ibge.gov.br/api/v3/malhas/estados/{uf}"
        "?formato=application/vnd.geo+json&intrarregiao=municipio"
    ),
    # v2 — fallback clássico (resolucao=5 para municípios, qualidade=4 para maior precisão)
    (
        "https://servicodados.ibge.gov.br/api/v2/malhas/{uf}"
        "?resolucao=5&formato=application/vnd.geo+json&qualidade=4"
    ),
]


def _extrair_br_municip(conteudo_zip: bytes) -> dict:
    """
    Extrai br_municip.cnv do TAB_SIH.zip (já baixado).
    Retorna dict {codigo_6d (str): nome_datasus (str)}.
    Filtra entradas 'MUNICIPIO IGNORADO' e códigos terminados em 9999/0000.
    """
    with zipfile.ZipFile(BytesIO(conteudo_zip)) as z:
        candidatos = [
            a for a in z.namelist()
            if 'br_municip' in a.lower() and a.lower().endswith('.cnv')
        ]
        if not candidatos:
            logger.warning("  br_municip.cnv não encontrado no ZIP")
            return {}

        conteudo = z.read(candidatos[0])
        registros = parse_cnv(conteudo)

    municipios = {}
    for r in registros:
        cod = r['codigo'].strip()
        desc = r['descricao'].strip()
        if 'IGNORADO' in desc.upper():
            continue
        if cod.endswith('0000') or cod.endswith('9999'):
            continue
        if len(cod) == 6 and cod.isdigit():
            municipios[cod] = desc

    logger.info(f"  br_municip.cnv: {len(municipios)} municípios válidos")
    return municipios


def _baixar_ibge_localidades() -> tuple:
    """
    Consulta API IBGE Localidades (1 chamada, todos os municípios).
    Retorna:
      - dict {codigo_6d (str): {'codigo_ibge': str, 'nome': str, 'estado': str}}
      - dict {codigo_uf_numerico (int): sigla (str)}  (ex: {43: 'RS'})

    FIX 2: A API v1 retorna hierarquia via microrregiao→mesorregiao→UF, mas a
    estrutura pode variar para municípios especiais (Brasília, Fernando de Noronha)
    ou em versões futuras da API. Acesso defensivo com fallback por código UF.
    """
    # Mapa código UF → sigla (fallback caso a estrutura aninhada falhe)
    _UF_FALLBACK = {
        11: 'RO', 12: 'AC', 13: 'AM', 14: 'RR', 15: 'PA', 16: 'AP', 17: 'TO',
        21: 'MA', 22: 'PI', 23: 'CE', 24: 'RN', 25: 'PB', 26: 'PE', 27: 'AL',
        28: 'SE', 29: 'BA', 31: 'MG', 32: 'ES', 33: 'RJ', 35: 'SP',
        41: 'PR', 42: 'SC', 43: 'RS', 50: 'MS', 51: 'MT', 52: 'GO', 53: 'DF',
    }

    logger.info("  Consultando API IBGE Localidades...")
    resp = requests.get(_URL_IBGE_LOCALIDADES, timeout=60)
    resp.raise_for_status()

    ibge = {}
    uf_map = {}  # {codigo_uf_int: sigla}
    erros_estrutura = 0

    for m in resp.json():
        cod7 = str(m['id'])
        cod6 = str(int(cod7) // 10)

        # Acesso defensivo — a API pode retornar microrregiao ou
        # regiao-imediata dependendo da versão/município
        uf_obj = None
        try:
            uf_obj = m['microrregiao']['mesorregiao']['UF']
        except (KeyError, TypeError):
            pass

        if uf_obj is None:
            # Fallback: extrair UF do código IBGE (2 primeiros dígitos)
            uf_cod = int(cod7[:2])
            sigla = _UF_FALLBACK.get(uf_cod, '')
            if not sigla:
                erros_estrutura += 1
                continue
            uf_obj = {'id': uf_cod, 'sigla': sigla}

        ibge[cod6] = {
            'codigo_ibge': cod7,
            'nome':        m['nome'],
            'estado':      uf_obj['sigla'],
        }
        uf_map[uf_obj['id']] = uf_obj['sigla']

    if erros_estrutura:
        logger.warning(f"  API IBGE: {erros_estrutura} municípios com estrutura inesperada (ignorados)")
    logger.info(f"  API IBGE: {len(ibge)} municípios, {len(uf_map)} UFs")
    return ibge, uf_map


def _centroide_polygon(coords: list) -> tuple:
    """Calcula centroide de um polígono GeoJSON (anel exterior)."""
    if not coords:
        return None, None
    anel = coords[0] if isinstance(coords[0][0], list) else coords
    n = len(anel)
    if n == 0:
        return None, None
    lon = sum(p[0] for p in anel) / n
    lat = sum(p[1] for p in anel) / n
    return round(lat, 4), round(lon, 4)


def _centroide_geometry(geom: dict) -> tuple:
    """Extrai centroide de qualquer geometria GeoJSON."""
    tipo = geom.get('type', '')
    coords = geom.get('coordinates', [])

    if tipo == 'Polygon':
        return _centroide_polygon(coords)
    elif tipo == 'MultiPolygon':
        maior = max(coords, key=lambda p: len(p[0]) if p else 0)
        return _centroide_polygon(maior)
    return None, None


def _baixar_centroides_ibge(uf_map: dict) -> dict:
    """
    Baixa centroides via API IBGE Malhas — GeoJSON por UF (27 chamadas).
    Tenta múltiplas versões da API (v3, v2) até encontrar uma que funcione.
    uf_map: {codigo_uf_numerico (int): sigla (str)}
    Retorna dict {codigo_ibge_7d (str): (latitude, longitude)}.
    """
    logger.info(f"  Baixando centroides IBGE Malhas ({len(uf_map)} UFs)...")
    centroides = {}
    session = requests.Session()
    ufs = sorted(uf_map.items())

    # Detecta qual URL funciona na primeira UF
    url_template = None
    for template in _URLS_IBGE_MALHAS_UF:
        test_uf_cod = ufs[0][0]
        test_url = template.format(uf=test_uf_cod)
        try:
            resp = session.get(test_url, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            if data.get('features') or data.get('type') == 'FeatureCollection':
                url_template = template
                logger.info(f"  API detectada: {template.split('?')[0].split('/api/')[1]}")
                break
        except Exception:
            continue

    if url_template is None:
        logger.warning("  Nenhuma API IBGE Malhas respondeu — centroides indisponíveis")
        session.close()
        return centroides

    for i, (uf_cod, uf_sigla) in enumerate(ufs, 1):
        url = url_template.format(uf=uf_cod)
        try:
            resp = session.get(url, timeout=60)
            resp.raise_for_status()
            geojson = resp.json()

            n_features = 0
            for feature in geojson.get('features', []):
                props = feature.get('properties', {})
                cod7 = str(props.get('codarea', props.get('cod', '')))
                geom = feature.get('geometry', {})
                if cod7 and geom:
                    lat, lon = _centroide_geometry(geom)
                    if lat is not None:
                        centroides[cod7] = (str(lat), str(lon))
                        n_features += 1

            logger.info(f"    [{i:02d}/{len(ufs)}] {uf_sigla}: {n_features} municípios")
        except Exception as e:
            logger.warning(f"    [{i:02d}/{len(ufs)}] {uf_sigla}: falha — {e}")

        # Cortesia: 0.3s entre chamadas
        if i < len(ufs):
            time.sleep(0.3)

    session.close()
    logger.info(f"  Centroides: {len(centroides)} municípios com coordenadas")
    return centroides


def baixar_municipios(conteudo_zip: bytes, saida_dir: Path, ufs_filtro: list = None) -> bool:
    """
    Gera municipios.csv cruzando três fontes oficiais:
      1. DATASUS br_municip.cnv — códigos 6d (chave do SIH)
      2. IBGE API Localidades   — códigos 7d + UF + nomes
      3. IBGE API Malhas (v3/v2) — centroides oficiais (GeoJSON por UF)
      4. regsaud.csv (já gerado) — região de saúde por município
    ufs_filtro: lista de siglas (ex: ['RS']) — se fornecido, baixa centroides
               apenas dessas UFs (economiza chamadas à API IBGE Malhas).
    Saída: municipios.csv com colunas do schema:
      CO_MUNICIPIO_6D, CO_MUNICIPIO_7D, NO_MUNICIPIO, SG_UF, NO_REGIAO_SAUDE, latitude, longitude
    """
    destino = saida_dir / Settings.SUPPORT_FILES["municipios"]
    if destino.exists():
        # Verificar se o arquivo existente tem coordenadas
        try:
            with open(destino, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                amostra = [next(reader) for _ in range(5)]
            tem_coords = any(r.get('latitude', '').strip() for r in amostra)
            if tem_coords:
                logger.info(f"  {destino.name} já existe (com coordenadas) — pulando")
                return True
            else:
                logger.info(f"  {destino.name} existe sem coordenadas — regenerando")
        except Exception:
            pass  # Se der erro ao ler, regenera

    logger.info("\n=== MUNICÍPIOS (DATASUS + IBGE) ===")

    # 1. DATASUS — códigos 6d usados no SIH
    datasus = _extrair_br_municip(conteudo_zip)

    # 2. IBGE API Localidades — código 7d, nome, UF
    try:
        ibge, uf_map = _baixar_ibge_localidades()
    except Exception as e:
        logger.warning(f"  Falha na API IBGE Localidades: {e}")
        return False

    # 3. IBGE API Malhas (v3/v2 fallback) — centroides apenas das UFs do escopo
    centroides = {}
    try:
        uf_map_filtrado = uf_map
        if ufs_filtro:
            ufs_upper = {u.upper() for u in ufs_filtro}
            uf_map_filtrado = {k: v for k, v in uf_map.items() if v.upper() in ufs_upper}
            logger.info(f"  Centroides: filtrando para {len(uf_map_filtrado)} UF(s): {sorted(uf_map_filtrado.values())}")
        centroides = _baixar_centroides_ibge(uf_map_filtrado)
    except Exception as e:
        logger.warning(f"  Falha na API IBGE Malhas: {e}")
        logger.warning("  Municípios serão salvos sem coordenadas")

    # 4. Região de saúde 
    regsaud = {}
    regsaud_path = saida_dir / TABELAS_ESPECIAIS.get('br_regsaud', 'regsaud.csv')
    if regsaud_path.exists():
        with open(regsaud_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cod = row.get('CODIGO', '').strip()
                desc = row.get('DESCRICAO', '').strip()
                if cod and desc:
                    # FIX: parse_cnv gruda código da região no nome
                    # ex: "11005 Zona da Mata" → "Zona da Mata"
                    partes = desc.split(None, 1)
                    if len(partes) == 2 and partes[0].isdigit():
                        desc = partes[1]
                    regsaud[cod] = desc
        logger.info(f"  regsaud.csv: {len(regsaud)} mapeamentos município→região")
    else:
        logger.warning("  regsaud.csv não encontrado — NO_REGIAO_SAUDE ficará vazio")

    # --- Cruzamento ---
    registros = []
    codigos_processados = set()

    for cod6, info in ibge.items():
        cod7 = info['codigo_ibge']
        lat, lon = centroides.get(cod7, ('', ''))
        regiao = regsaud.get(cod6, '')
        registros.append((cod6, cod7, info['nome'], info['estado'], regiao, lat, lon))
        codigos_processados.add(cod6)

    # Códigos DATASUS sem correspondência IBGE 
    extras = 0
    for cod6, nome_datasus in datasus.items():
        if cod6 not in codigos_processados:
            uf_cod = cod6[:2]
            regiao = regsaud.get(cod6, '')
            registros.append((cod6, '', nome_datasus.title(), uf_cod, regiao, '', ''))
            extras += 1

    registros.sort(key=lambda x: x[0])

    with open(destino, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'CO_MUNICIPIO_6D', 'CO_MUNICIPIO_7D', 'NO_MUNICIPIO',
            'SG_UF', 'NO_REGIAO_SAUDE', 'latitude', 'longitude',
        ])
        writer.writerows(registros)

    n_coords = sum(1 for r in registros if r[5])
    n_regsaud = sum(1 for r in registros if r[4])
    logger.info(f"  {destino.name:<25s} {len(registros):>6,} municípios "
                f"({n_coords} coords, {n_regsaud} regsaud)")
    if extras:
        logger.info(f"  {extras} códigos DATASUS sem correspondência IBGE")

    return True


def main():
    saida_dir = Settings.SUPPORT_FILES_DIR
    saida_dir.mkdir(parents=True, exist_ok=True)
    logger.info("=== DOWNLOAD TABELAS DOMÍNIO DATASUS ===")
    logger.info(f"Destino: {saida_dir}")
    conteudo_zip = baixar_zip()
    processar_zip(conteudo_zip, saida_dir)
    baixar_municipios(conteudo_zip, saida_dir, ufs_filtro=None)  # todas as UFs — coordenadas completas


if __name__ == "__main__":
    from config.logging_config import setup_logging
    setup_logging("download_tabelas")
    main()