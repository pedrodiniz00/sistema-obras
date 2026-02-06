import streamlit as st
import pandas as pd
import sqlite3
import pdfplumber
from io import BytesIO
from fpdf import FPDF
from datetime import date, timedelta, datetime

st.set_page_config(page_title="Gestor Obras v19 (Cronológico)", layout="wide")

# --- BANCO DE DADOS ---
def conectar():
    return sqlite3.connect('gestao_obras_v16.db')

def criar_tabelas():
    conn = conectar()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS obras
                 (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                  nome TEXT UNIQUE, status TEXT DEFAULT 'ATIVA', 
                  area_m2 REAL DEFAULT 0, data_inicio TEXT,
                  pdf_nome TEXT, pdf_blob BLOB)''')
    c.execute('''CREATE TABLE IF NOT EXISTS custos
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  obra_nome TEXT, data TEXT, item TEXT, qtd REAL, 
                  unidade TEXT, valor_un REAL, total REAL, 
                  classe TEXT, etapa TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS cronograma
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                  obra_nome TEXT, etapa TEXT, dias_estimados INTEGER,
                  data_inicio TEXT, data_fim TEXT, 
                  porcentagem INTEGER DEFAULT 0)''')
    conn.commit()
    conn.close()

# --- FUNÇÕES ---
def salvar_projeto_pdf(nome_obra, arquivo_upload):
    conn = conectar()
    dados_binarios = arquivo_upload.read()
    nome_arquivo = arquivo_upload.name
    conn.execute("UPDATE obras SET pdf_nome = ?, pdf_blob = ? WHERE nome = ?", 
                 (nome_arquivo, dados_binarios, nome_obra))
    conn.commit()
    conn.close()

def recuperar_pdf(nome_obra):
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT pdf_nome, pdf_blob FROM obras WHERE nome = ?", (nome_obra,))
    res = c.fetchone()
    conn.close()
    return res if res else (None, None)

def excluir_obra_completa(nome_obra):
    conn = conectar()
    try:
        conn.execute("DELETE FROM cronograma WHERE obra_nome = ?", (nome_obra,))
        conn.execute("DELETE FROM custos WHERE obra_nome = ?", (nome_obra,))
        conn.execute("DELETE FROM obras WHERE nome = ?", (nome_obra,))
        conn.commit()
        return True
    except: return False
    finally: conn.close()

def adicionar_etapa_manual(obra, nome_etapa, d_inicio, d_fim):
    conn = conectar()
    dias = (d_fim - d_inicio).days
    # Salva datas no formato ISO (YYYY-MM-DD) para ordenação funcionar
    conn.execute("INSERT INTO cronograma (obra_nome, etapa, dias_estimados, data_inicio, data_fim, porcentagem) VALUES (?, ?, ?, ?, ?, 0)",
                 (obra, nome_etapa, dias, d_inicio, d_fim))
    conn.commit()
    conn.close()

def atualizar_datas_etapa(id_etapa, nova_inicio, nova_fim):
    conn = conectar()
    d1 = datetime.strptime(str(nova_inicio), "%Y-%m-%d")
    d2 = datetime.strptime(str(nova_fim), "%Y-%m-%d")
    dias = (d2 - d1).days
    conn.execute("UPDATE cronograma SET data_inicio = ?, data_fim = ?, dias_estimados = ? WHERE id = ?", 
                 (nova_inicio, nova_fim, dias, id_etapa))
    conn.commit()
    conn.close()

def excluir_etapa(id_etapa):
    conn = conectar()
    conn.execute("DELETE FROM cronograma WHERE id = ?", (id_etapa,))
    conn.commit()
    conn.close()

def gerar_cronograma_automatico(obra, area, pedreiros, ajudantes, data_inicio_obra):
    horas_dia_equipe = (pedreiros * 8 + ajudantes * 8) * 0.80
    if horas_dia_equipe == 0: return
    
    etapas_modelo = [
        ("Serviços Preliminares", 2.0), ("Fundação", 6.0),
        ("Estrutura/Alvenaria", 12.0), ("Telhado", 5.0),
        ("Instalações", 4.0), ("Reboco", 8.0),
        ("Pisos", 6.0), ("Pintura", 5.0)
    ]
    
    conn = conectar()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM cronograma WHERE obra_nome = ?", (obra,))
    
    data_atual = datetime.strptime(str(data_inicio_obra), "%Y-%m-%d").date()
    
    for nome_etapa, indice in etapas_modelo:
        total_horas = area * indice
        dias_necessarios = max(2, int(total_horas / horas_dia_equipe))
        data_fim = data_atual + timedelta(days=dias_necessarios)
        cursor.execute("INSERT INTO cronograma (obra_nome, etapa, dias_estimados, data_inicio, data_fim, porcentagem) VALUES (?, ?, ?, ?, ?, 0)",
                       (obra, nome_etapa, dias_necessarios, data_atual, data_fim))
        data_atual = data_fim
    conn.commit()
    conn.close()

def criar_obra(nome, area, data_in):
    conn = conectar()
    try:
        conn.execute("INSERT INTO obras (nome, status, area_m2, data_inicio) VALUES (?, 'ATIVA', ?, ?)", (nome, area, data_in))
        conn.commit()
        st.sidebar.success(f"Obra '{nome}' criada!")
    except: st.sidebar.warning("Obra já existe!")
    finally: conn.close()

def listar_obras():
    conn = conectar()
    df = pd.read_sql_query("SELECT nome FROM obras", conn)
    conn.close()
    return df['nome'].tolist()

def get_dados_obra(nome):
    conn = conectar()
    c = conn.cursor()
    c.execute("SELECT area_m2, data_inicio, status FROM obras WHERE nome=?", (nome,))
    r = c.fetchone()
    conn.close()
    return r if r else (0, date.today(), 'ATIVA')

def salvar_gasto(obra, data, item, qtd, un, valor, classe, etapa):
    conn = conectar()
    conn.execute("INSERT INTO custos (obra_nome, data, item, qtd, unidade, valor_un, total, classe, etapa) VALUES (?,?,?,?,?,?,?,?,?)",
                 (obra, data, item, qtd, un, valor, qtd*valor, classe, etapa))
    conn.commit()
    conn.close()

def ler_custos(obra):
    conn = conectar()
    df = pd.read_sql_query("SELECT * FROM custos WHERE obra_nome = ? ORDER BY data DESC", conn, params=(obra,))
    conn.close()
    return df

def ler_cronograma(obra):
    conn = conectar()
    # AQUI ESTÁ O SEGREDO: ORDER BY data_inicio ASC
    # Isso garante que o banco de dados sempre entregue a lista ordenada cronologicamente
    df = pd.read_sql_query("SELECT * FROM cronograma WHERE obra_nome = ? ORDER BY data_inicio ASC, data_fim ASC", conn, params=(obra,))
    conn.close()
    return df

def atualizar_porcentagem_etapa(id_etapa, nova_porcentagem):
    conn = conectar()
    conn.execute("UPDATE cronograma SET porcentagem = ? WHERE id = ?", (nova_porcentagem, id_etapa))
    conn.commit()
    conn.close()

def ler_pdf_plumber(arquivo_bytes):
    texto_extraido = ""
    resumo_materiais = []
    with pdfplumber.open(BytesIO(arquivo_bytes)) as pdf:
        for page in pdf.pages:
            texto = page.extract_text()
            if texto:
                texto_extraido += texto + "\n"
                linhas = texto.split('\n')
                for linha in linhas:
                    if any(unid in linha.lower() for unid in ['m²', 'm2', 'kg', 'sacos', 'unid', 'total']):
                        resumo_materiais.append(linha)
    return texto_extraido, resumo_materiais

# --- APP ---
criar_tabelas()
if 'confirmar_exclusao_obra' not in st.session_state: st.session_state.confirmar_exclusao_obra = False
if 'f_item' not in st.session_state: st.session_state.f_item = ""

st.sidebar.title("🏗️ Gestor Obras v19")

# 1. Nova Obra
with st.sidebar.expander("➕ Nova Obra"):
    n_nome = st.text_input("Nome")
    n_area = st.number_input("Área (m²)", 50.0)
    n_data = st.date_input("Início", format="DD/MM/YYYY")
    if st.button("Criar"):
        if n_nome: 
            criar_obra(n_nome, n_area, n_data)
            st.rerun()

obras = listar_obras()
if not obras:
    st.warning("Cadastre sua primeira obra!")
    st.stop()

# 2. Seleção
st.sidebar.divider()
obra_atual = st.sidebar.selectbox("Obra Ativa:", obras)
dados = get_dados_obra(obra_atual)
area_obra = dados[0]
data_inicio = dados[1]

# 3. Exclusão
if st.sidebar.button("🗑️ Excluir Obra"):
    st.session_state.confirmar_exclusao_obra = True
if st.session_state.confirmar_exclusao_obra:
    st.sidebar.error(f"Apagar **{obra_atual}**?")
    c1, c2 = st.sidebar.columns(2)
    if c1.button("✅ SIM"):
        excluir_obra_completa(obra_atual)
        st.session_state.confirmar_exclusao_obra = False
        st.rerun()
    if c2.button("❌ NÃO"):
        st.session_state.confirmar_exclusao_obra = False
        st.rerun()

st.title(f"Painel: {obra_atual}")
aba1, aba2, aba3 = st.tabs(["📅 Cronograma & Status", "💰 Financeiro", "🧮 Calculadora"])

# --- ABA 1: CRONOGRAMA ---
with aba1:
    col_proj, col_crono = st.columns([1, 2])
    
    with col_proj:
        st.subheader("📂 Projeto")
        pdf_nome, pdf_blob = recuperar_pdf(obra_atual)
        if pdf_nome:
            st.success(f"Arquivo: {pdf_nome}")
            st.download_button("📥 Baixar", pdf_blob, pdf_nome, "application/pdf")
            if st.button("🧠 Ler PDF"):
                txt, items = ler_pdf_plumber(pdf_blob)
                st.session_state['temp_items'] = items
        
        novo_pdf = st.file_uploader("Novo PDF", type="pdf")
        if novo_pdf and st.button("Salvar PDF"):
            salvar_projeto_pdf(obra_atual, novo_pdf)
            st.rerun()
            
        if 'temp_items' in st.session_state:
            st.divider()
            st.write("Resumo Detectado:")
            for i in st.session_state['temp_items']: st.code(i)

    with col_crono:
        st.subheader("📅 Gestão e Prazos")
        
        df = ler_cronograma(obra_atual)
        
        if not df.empty:
            total_pts = len(df) * 100
            atuais = df['porcentagem'].sum()
            prog_global = (atuais / total_pts) * 100 if total_pts > 0 else 0
            
            hoje = date.today().strftime("%Y-%m-%d")
            atrasados = df[ (df['porcentagem'] < 100) & (df['data_fim'] < hoje) ]
            qtd_atrasos = len(atrasados)
            
            k1, k2 = st.columns(2)
            k1.metric("Progresso Total", f"{int(prog_global)}%")
            
            if qtd_atrasos > 0:
                k2.error(f"🔴 ATRASADA ({qtd_atrasos} etapas vencidas)")
            else:
                k2.success("🟢 NO PRAZO")
                
            st.progress(prog_global / 100)
            st.divider()

        # Ferramentas
        with st.expander("Ferramentas de Etapas"):
            st.write("**Automático:**")
            c_a, c_b, c_c = st.columns([1, 1, 1])
            qp = c_a.number_input("Pedreiros", 1, 20, 2)
            qa = c_b.number_input("Ajudantes", 1, 20, 2)
            if c_c.button("Gerar Padrão"):
                gerar_cronograma_automatico(obra_atual, area_obra, qp, qa, data_inicio)
                st.rerun()
            
            st.divider()
            st.write("**Manual:**")
            c_add1, c_add2, c_add3 = st.columns([2, 1, 1])
            nova_etapa_nome = c_add1.text_input("Nome")
            nova_ini = c_add2.date_input("Início", value=date.today(), format="DD/MM/YYYY")
            nova_fim = c_add3.date_input("Fim", value=date.today() + timedelta(days=5), format="DD/MM/YYYY")
            if st.button("Adicionar"):
                adicionar_etapa_manual(obra_atual, nova_etapa_nome, nova_ini, nova_fim)
                st.rerun()

        # Lista de Etapas
        if not df.empty:
            st.markdown("---")
            hoje = date.today().strftime("%Y-%m-%d")

            for i, row in df.iterrows():
                c_head, c_dates, c_slider = st.columns([1.5, 2, 1.5])
                
                with c_head:
                    st.write(f"**{row['etapa']}**")
                    if st.button("🗑️", key=f"del_{row['id']}"):
                        excluir_etapa(row['id'])
                        st.rerun()
                    
                    if row['porcentagem'] == 100: st.success("✅ CONCLUÍDO")
                    elif row['porcentagem'] == 0: st.caption("🔴 Pendente")
                    else: st.caption(f"🟡 {row['porcentagem']}%")

                with c_dates:
                    d_ini_obj = datetime.strptime(row['data_inicio'], "%Y-%m-%d").date()
                    d_fim_obj = datetime.strptime(row['data_fim'], "%Y-%m-%d").date()
                    
                    d1 = st.date_input("Início", d_ini_obj, key=f"ini_{row['id']}", label_visibility="collapsed", format="DD/MM/YYYY")
                    d2 = st.date_input("Fim", d_fim_obj, key=f"fim_{row['id']}", label_visibility="collapsed", format="DD/MM/YYYY")
                    st.caption(f"🗓️ {d_ini_obj.strftime('%d/%m/%y')} ➡ {d_fim_obj.strftime('%d/%m/%y')}")

                    if d1 != d_ini_obj or d2 != d_fim_obj:
                        atualizar_datas_etapa(row['id'], d1, d2)
                        st.rerun()
                        
                    if row['porcentagem'] < 100 and str(d2) < hoje:
                        st.error("ATRASADO")

                with c_slider:
                    novo = st.slider("Avanço", 0, 100, int(row['porcentagem']), key=f"sld_{row['id']}")
                    if novo != row['porcentagem']:
                        atualizar_porcentagem_etapa(row['id'], novo)
                        st.rerun()
                st.divider()
        else:
            st.info("Nenhuma etapa definida.")

# --- ABA 2: FINANCEIRO ---
with aba2:
    with st.expander("Novo Gasto", expanded=True):
        c1, c2, c3 = st.columns(3)
        dt = c1.date_input("Data", key="f_data", format="DD/MM/YYYY")
        cl = c1.selectbox("Classe", ["Materiais", "Mão de Obra", "Equipamentos"], key="f_classe")
        et = c2.selectbox("Etapa", ["Geral", "Fundação", "Alvenaria", "Acabamento"], key="f_etapa")
        it = c2.text_input("Descrição", key="f_item")
        qt = c3.number_input("Qtd", 0.0, key="f_qtd")
        un = c3.selectbox("Un", ["unid", "kg", "m²"], key="f_un")
        vl = c3.number_input("Valor", 0.0, key="f_valor")
        
        def save():
            salvar_gasto(obra_atual, st.session_state.f_data, st.session_state.f_item, 
                         st.session_state.f_qtd, st.session_state.f_un, 
                         st.session_state.f_valor, st.session_state.f_classe, 
                         st.session_state.f_etapa)
            st.session_state.f_item = ""
            st.session_state.f_qtd = 1.0
            st.session_state.f_valor = 0.0
        st.button("Salvar Gasto", on_click=save)

    df_fin = ler_custos(obra_atual)
    if not df_fin.empty:
        st.divider()
        st.metric("Total Gasto", f"R$ {df_fin['total'].sum():,.2f}")
        df_display = df_fin.copy()
        df_display['data'] = pd.to_datetime(df_display['data']).dt.strftime('%d/%m/%y')
        st.dataframe(df_display.drop(columns=['id', 'obra_nome']), use_container_width=True)

# --- ABA 3: CALCULADORA ---
with aba3:
    tp = st.radio("Calc:", ["Concreto", "Reboco"], horizontal=True)
    if tp == "Concreto":
        tr = st.selectbox("Traço", ["1:2:3", "1:3:6"])
        v = st.number_input("Vol m³")
        if st.button("Calc"): st.info(f"Cimento: {v*(7 if '1:2:3' in tr else 4):.1f} sc")
    else:
        a = st.number_input("Area m²")
        e = st.number_input("Esp cm", 2.0)
        if st.button("Calc"): st.info(f"Cimento: {a*(e/100)*6:.1f} sc")